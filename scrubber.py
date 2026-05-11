"""
Post-run data scrubber: merge all output CSVs, deduplicate by parcel_id,
split into homeowners vs businesses, and sort by assessed value descending.

Skips any CSV whose name starts with SCRUBBED_ so re-running is safe.

Usage (CLI):
    python -m processors.scrubber
    python -m processors.scrubber --input C:/path/to/folder
    python -m processors.scrubber --min-value 400000
    python -m processors.scrubber --county Dallas

Callable from app.py:
    from processors.scrubber import scrub
    result = scrub()
"""

import argparse
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

HOME = Path.home()
OUTPUT_DIR = HOME / "sunforge" / "output"

_BIZ_PATTERN = re.compile(
    r"\b(llc|inc|corp|ltd|lp|lllp|trust|hoa|assoc|association|bank|church|"
    r"school|isd|holdings|properties|investments|realty|ventures|partners|"
    r"enterprises|group|management|services|solutions|county|city|state)\b",
    re.IGNORECASE,
)


def _to_numeric_value(val) -> float:
    """Convert '$1,234,567' → 1234567.0. Returns -1.0 if unparseable."""
    if pd.isna(val) or str(val).strip() in ("", "-", "N/A", "None"):
        return -1.0
    cleaned = re.sub(r"[$,\s]", "", str(val))
    try:
        return float(cleaned)
    except ValueError:
        return -1.0


def scrub(
    input_dir: Path = OUTPUT_DIR,
    min_value: float = 0,
    county_filter: str = None,
) -> dict:
    """
    Merge, deduplicate, and split all CSVs in input_dir.

    Args:
        input_dir:     Folder containing SunForge output CSVs.
        min_value:     Drop records below this assessed value (0 = keep all).
        county_filter: If set, only keep records matching this county name.

    Returns:
        Dict with keys: raw, deduped, homeowners, businesses,
                        homeowners_path, businesses_path.
    """
    csv_files = sorted(input_dir.glob("*.csv"))
    csv_files = [f for f in csv_files if not f.name.startswith("SCRUBBED_")]

    if not csv_files:
        return {"error": f"No CSV files found in {input_dir}"}

    # ── Load ──────────────────────────────────────────────────────────────────
    dfs = []
    skipped = []
    for f in csv_files:
        try:
            dfs.append(pd.read_csv(f, on_bad_lines="skip", dtype=str, low_memory=False))
        except Exception as e:
            skipped.append(f"{f.name}: {e}")

    if not dfs:
        return {"error": "All CSV files failed to load", "skipped": skipped}

    df = pd.concat(dfs, ignore_index=True)
    raw_count = len(df)

    # ── Artifact rows ─────────────────────────────────────────────────────────
    if "owner_name" in df.columns:
        df = df[~df["owner_name"].astype(str).str.contains(
            r"Page\s+\d+\s+of\s+\d+", na=False, regex=True
        )]

    # ── Optional county filter ────────────────────────────────────────────────
    if county_filter and "county" in df.columns:
        df = df[df["county"].astype(str).str.lower() == county_filter.lower()]

    # ── Deduplicate ───────────────────────────────────────────────────────────
    if "parcel_id" in df.columns:
        df["_key"] = (
            df["parcel_id"].astype(str)
            .str.strip().str.upper()
            .str.replace(r"[-\s]", "", regex=True)
        )
        valid_key = df["_key"].str.len() > 0
        # For records with a real parcel_id: sort by scraped_at so keep='last'
        # retains the newest version; parcel_ids that are blank use fallback.
        if "scraped_at" in df.columns:
            df = df.sort_values("scraped_at", na_position="first")
        df_with_key = df[valid_key].drop_duplicates(subset=["_key"], keep="last")
        df_no_key = df[~valid_key].drop_duplicates(
            subset=["owner_name", "property_address"], keep="last"
        )
        df = pd.concat([df_with_key, df_no_key], ignore_index=True)
        df = df.drop(columns=["_key"])
    else:
        df = df.drop_duplicates(
            subset=["owner_name", "property_address"], keep="last"
        )

    deduped_count = len(df)

    # ── Numeric value column for filtering + sorting ──────────────────────────
    df["_val"] = df["assessed_value"].apply(_to_numeric_value) \
        if "assessed_value" in df.columns \
        else pd.Series(-1.0, index=df.index)

    if min_value > 0:
        df = df[df["_val"] >= min_value]

    # ── Classify: homeowner vs business ───────────────────────────────────────
    is_residential = (
        df["property_type"].astype(str).str.lower()
        .str.contains(r"residential|single.?family|sfr", na=False, regex=True)
        if "property_type" in df.columns
        else pd.Series(True, index=df.index)
    )
    has_biz_name = (
        df["business_name"].astype(str).str.strip()
        .replace({"nan": "", "None": ""}).ne("")
        if "business_name" in df.columns
        else pd.Series(False, index=df.index)
    )
    owner_looks_like_biz = (
        df["owner_name"].astype(str).str.contains(_BIZ_PATTERN, na=False)
        if "owner_name" in df.columns
        else pd.Series(False, index=df.index)
    )

    homeowner_mask = is_residential & ~has_biz_name & ~owner_looks_like_biz
    homeowners = df[homeowner_mask].copy()
    businesses = df[~homeowner_mask].copy()

    homeowners["record_type"] = "homeowner"
    businesses["record_type"] = "business"

    homeowners = homeowners.sort_values("_val", ascending=False, na_position="last").drop(columns=["_val"])
    businesses = businesses.sort_values("_val", ascending=False, na_position="last").drop(columns=["_val"])

    # ── Write clean output CSVs ───────────────────────────────────────────────
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{county_filter.upper()}" if county_filter else ""

    hw_path  = input_dir / f"SCRUBBED_homeowners{suffix}_{date_str}.csv"
    biz_path = input_dir / f"SCRUBBED_businesses{suffix}_{date_str}.csv"

    homeowners.to_csv(hw_path,  index=False)
    businesses.to_csv(biz_path, index=False)

    # ── Archive raw source files ───────────────────────────────────────────────
    # Move the raw scraper CSVs and JSONs into output/archive/ so the output
    # folder stays clean and the scrubber won't reprocess them next run.
    # Files are NOT deleted — archive/ keeps them for reference or recovery.
    archive_dir = input_dir / "archive"
    archive_dir.mkdir(exist_ok=True)

    archived = []
    for f in csv_files:
        try:
            dest = archive_dir / f.name
            # If a file with the same name already exists in archive, version it
            if dest.exists():
                dest = archive_dir / f"{f.stem}_dup_{date_str}{f.suffix}"
            f.rename(dest)
            archived.append(f.name)
            # Move the matching JSON sidecar if present
            json_sidecar = f.with_suffix(".json")
            if json_sidecar.exists():
                json_sidecar.rename(archive_dir / json_sidecar.name)
        except Exception:
            pass  # Don't fail the scrub if a file can't be moved

    return {
        "raw": raw_count,
        "deduped": deduped_count,
        "filtered": len(df),
        "homeowners": len(homeowners),
        "businesses": len(businesses),
        "homeowners_path": str(hw_path),
        "businesses_path": str(biz_path),
        "archived": archived,
        "skipped_files": skipped,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrub and split SunForge output CSVs"
    )
    parser.add_argument(
        "--input", type=Path, default=OUTPUT_DIR,
        help="Folder containing CSVs (default: ~/sunforge/output)",
    )
    parser.add_argument(
        "--min-value", type=float, default=0,
        help="Minimum assessed value to keep (e.g. 400000)",
    )
    parser.add_argument(
        "--county", type=str, default=None,
        help="Only process records from this county (e.g. Dallas)",
    )
    args = parser.parse_args()

    result = scrub(args.input, args.min_value, args.county)

    if "error" in result:
        print(f"Error: {result['error']}")
    else:
        print(f"\n✅ Scrub complete")
        print(f"   Raw records loaded : {result['raw']:,}")
        print(f"   After dedup        : {result['deduped']:,}")
        if args.min_value > 0:
            print(f"   After value filter : {result['filtered']:,}")
        print(f"   Homeowners         : {result['homeowners']:,}  →  {Path(result['homeowners_path']).name}")
        print(f"   Businesses         : {result['businesses']:,}  →  {Path(result['businesses_path']).name}")
        if result["skipped_files"]:
            print(f"\n   ⚠ Skipped files:")
            for s in result["skipped_files"]:
                print(f"     {s}")

"""
Standalone scrubber server — serves scrubber.html at http://localhost:7434
and exposes the /api/scrub + /api/scrub-status endpoints.

Run: python serve.py
Or use it embedded in SunForge at http://localhost:7433/scrub
"""
import re
import json
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS

app = Flask(__name__, static_folder=".", static_url_path="")
CORS(app)

HOME       = Path.home()
OUTPUT_DIR = HOME / "sunforge" / "output"


@app.route("/")
def index():
    return send_file("scrubber.html")


@app.route("/api/scrub-status")
def api_scrub_status():
    scrubbed_counties = set()
    all_scrubbed = False
    for f in OUTPUT_DIR.glob("SCRUBBED_homeowners*.csv"):
        parts = f.stem.split("_")
        if len(parts) >= 3:
            candidate = parts[2]
            if candidate.isdigit():
                all_scrubbed = True
            else:
                scrubbed_counties.add(candidate.title())
    return jsonify({"all_scrubbed": all_scrubbed, "counties": sorted(scrubbed_counties)})


@app.route("/api/scrub", methods=["POST"])
def api_scrub():
    body     = request.json or {}
    min_val  = float(body.get("min_value", 0))
    county   = body.get("county") or None
    try:
        from scrubber import scrub
        result = scrub(OUTPUT_DIR, min_value=min_val, county_filter=county)
    except ImportError:
        return jsonify({"error": "pandas required — pip install pandas>=2.0"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    if "error" in result:
        return jsonify(result), 400
    return jsonify(result)


@app.route("/api/output-file/<path:relpath>")
def output_file(relpath):
    target = HOME / "sunforge" / relpath
    if not target.exists():
        return jsonify({"error": "file not found"}), 404
    return send_file(str(target), as_attachment=True)


if __name__ == "__main__":
    print(f"Scrubber UI → http://localhost:7434")
    app.run(host="0.0.0.0", port=7434, debug=False)

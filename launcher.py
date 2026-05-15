"""
SunForge Scrubber launcher — entry point for the packaged .exe.

Starts the Flask scrubber server in a background daemon thread, waits for it
to be ready, then opens a native pywebview window. Closing the window exits
the process; the Flask thread is a daemon so it dies with the process.
"""
import os
import sys
import time
import threading
from pathlib import Path
import traceback

# ── Path resolution ───────────────────────────────────────────────────────────
if getattr(sys, 'frozen', False):
    _BUNDLE_DIR = Path(sys._MEIPASS)
    _DATA_DIR   = Path(os.environ.get('APPDATA', str(Path.home()))) / 'SunForge'
else:
    _BUNDLE_DIR = Path(__file__).parent
    _DATA_DIR   = Path(os.environ.get('APPDATA', str(Path.home()))) / 'SunForge'

_STARTUP_LOG = _DATA_DIR / 'scrubber_startup.log'
_GUI_ENV = 'SUNFORGE_SCRUBBER_GUI'

def _log(msg):
    try:
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        with _STARTUP_LOG.open('a', encoding='utf-8') as fp:
            fp.write(f'{time.strftime("%Y-%m-%d %H:%M:%S")} {msg}\n')
    except Exception as exc:
        # sys.stderr is None in windowed PyInstaller builds — guard before writing
        stderr = sys.stderr or sys.__stderr__
        if stderr is not None:
            print(f'Unable to write startup log {_STARTUP_LOG}: {exc}', file=stderr)

# Ensure the scrubber module and HTML are importable / findable
sys.path.insert(0, str(_BUNDLE_DIR))

# ── Constants ─────────────────────────────────────────────────────────────────
_PORT    = 7434
_HOST    = '127.0.0.1'
_URL     = f'http://{_HOST}:{_PORT}'
_TIMEOUT = 20

# ── Flask thread ──────────────────────────────────────────────────────────────
def _run_flask():
    _log(f'Starting Flask scrubber on {_URL}')
    try:
        import serve
        serve.app.run(host=_HOST, port=_PORT, threaded=True, use_reloader=False, debug=False)
    except Exception:
        _log('Flask failed:\n' + traceback.format_exc())

def _wait_for_flask(timeout=_TIMEOUT):
    import urllib.request
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(f'{_URL}/api/scrub-status', timeout=1)
            return True
        except Exception:
            time.sleep(0.15)
    return False

def _alert(title, message):
    try:
        import tkinter, tkinter.messagebox
        root = tkinter.Tk()
        root.withdraw()
        tkinter.messagebox.showerror(title, message)
    except Exception:
        _log('Unable to show startup alert:\n' + traceback.format_exc())

def _start_webview(webview):
    gui = os.environ.get(_GUI_ENV, '').strip()
    if gui:
        _log(f'Starting pywebview with {_GUI_ENV}={gui}')
        webview.start(gui=gui, debug=False)
        return

    _log('Starting pywebview with platform default GUI backend')
    webview.start(debug=False)

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == '__main__':
    _log('Scrubber launcher starting')

    flask_thread = threading.Thread(target=_run_flask, daemon=True, name='flask-scrubber')
    flask_thread.start()

    if not _wait_for_flask():
        _log('Flask did not respond before timeout')
        _alert(
            'SunForge Scrubber — startup error',
            f'Scrubber server did not start within {_TIMEOUT}s.\n'
            f'Make sure port {_PORT} is not already in use.',
        )
        sys.exit(1)

    import webview
    _log('Opening pywebview window')
    webview.create_window(
        title='SunForge Scrubber',
        url=_URL,
        width=860,
        height=700,
        min_size=(720, 540),
        text_select=True,
    )
    try:
        _start_webview(webview)
    except Exception:
        error = traceback.format_exc()
        _log('pywebview failed:\n' + error)
        _alert(
            'SunForge Scrubber — window error',
            'The Scrubber window could not be opened. Check the startup log for details.',
        )
        raise
    _log('Window closed')
    sys.exit(0)

"""Offline integrity check: validates the bundled library and bot source."""
from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parent
files = [ROOT / "main.py", ROOT / "config.py", ROOT / "database.py", ROOT / "utils.py", ROOT / "speaker_api.py", ROOT / "group_registry_server.py"]
for path in files:
    ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

for forbidden in ("from spluspy import", "from spluspy.sessions import", "import spluspy._updates"):
    text = (ROOT / "main.py").read_text(encoding="utf-8")
    if forbidden in text:
        raise SystemExit(f"Forbidden legacy import found: {forbidden}")

if not (ROOT / "spluslib" / "__init__.py").exists():
    raise SystemExit("Bundled spluslib package is missing")
print("OK: syntax, bundled SplusLib, and legacy-import checks passed.")

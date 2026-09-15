#!/usr/bin/env bash
set -euo pipefail
INTERPRETER_JSON="/opt/notebook_workspace/conf/interpreter.json"
if [ -f "$INTERPRETER_JSON" ]; then
  python3 - "$INTERPRETER_JSON" <<'PYEOF'
import json, os, sys
path = sys.argv[1]
with open(path) as f:
    data = json.load(f)
jdbc = data.get("interpreterSettings", {}).get("jdbc", {}).get("properties", {})
if "default.user" in jdbc:
    jdbc["default.user"]["value"] = os.environ.get("POSTGIS_USER", "maritime")
if "default.password" in jdbc:
    jdbc["default.password"]["value"] = os.environ.get("POSTGIS_PASSWORD", "maritime")
if "default.url" in jdbc:
    jdbc["default.url"]["value"] = (
        f"jdbc:postgresql://{os.environ.get('POSTGIS_HOST','postgis')}:"
        f"{os.environ.get('POSTGIS_PORT','5432')}/{os.environ.get('POSTGIS_DB','maritime')}"
    )
with open(path, "w") as f:
    json.dump(data, f, indent=2)
PYEOF
fi
exec "$@"

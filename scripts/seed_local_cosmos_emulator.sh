#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${1:-https://localhost:8081/}"
KEY="${COSMOS_KEY:-C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOADER_PATH="$REPO_ROOT/services/cosmos-emulator/load_data.py"
INIT_FILE_PATH="$REPO_ROOT/services/cosmos-emulator/cosmos-init.sql"
REQUIREMENTS_PATH="$REPO_ROOT/services/cosmos-emulator/requirements.txt"

if [[ ! -f "$LOADER_PATH" ]]; then
  echo "Loader script not found: $LOADER_PATH" >&2
  exit 1
fi

if [[ ! -f "$INIT_FILE_PATH" ]]; then
  echo "Init SQL file not found: $INIT_FILE_PATH" >&2
  exit 1
fi

PYTHON_EXE="$REPO_ROOT/.venv/Scripts/python.exe"
if [[ ! -f "$PYTHON_EXE" ]]; then
  PYTHON_EXE="python"
fi

"$PYTHON_EXE" -m pip install -r "$REQUIREMENTS_PATH"
"$PYTHON_EXE" "$LOADER_PATH" "$INIT_FILE_PATH" --endpoint "$ENDPOINT" --key "$KEY"

echo "Local Cosmos emulator seeded successfully."
echo "Containers created/updated: factory_ops.shift_data, factory_ops.kpi_data"

#!/bin/sh
set -eu

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$PROJECT_ROOT"

exec /usr/local/bin/python3 -m pytest -q tests/test_export_public_sheet.py

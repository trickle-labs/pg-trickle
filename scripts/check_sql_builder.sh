#!/usr/bin/env bash
set -euo pipefail
exec python3 "$(dirname "${BASH_SOURCE[0]}")/check_sql_builder.py" "${1:-$(dirname "${BASH_SOURCE[0]}")/../src}"

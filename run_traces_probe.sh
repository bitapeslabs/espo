#!/bin/bash
set -euo pipefail

cargo run --quiet --bin traces_probe -- \
  --readonly-metashrew-db-dir /data/.metashrew/v9/.metashrew-v9 \
  --secondary-path ./db/tmp/metashrew-probe \
  "$@"

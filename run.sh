cargo run --release --features binary -- \
  --config-path ./config.json \
  --view-only
# Add --view-only to disable indexing/mempool and serve existing data only.

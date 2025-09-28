echo "[SETUP] resetting db..."
rm -rf $HOME/espo/db/espo/*
cp -r $HOME/bkp-espo/* $HOME/espo/db/espo/
echo "[SETUP] starting espo..."
cargo run --release -- \
  --readonly-metashrew-db-dir /data/.metashrew/mainnet-v2.0.0/.metashrew-reconcile \
  --port 5778 \
  --electrum-rpc-url 0.0.0.0:50015 \
  --bitcoind-rpc-url http://127.0.0.1:8332 \
  --bitcoind-rpc-user admin \
  --bitcoind-rpc-pass admin \
  --bitcoind-blocks-dir "$HOME/.bitcoin/blocks"

cargo run --release -- \
  --readonly-metashrew-db-dir /data/.metashrew/mainnet-v2.0.0/.metashrew-reconcile \
  --port 5778 \
  --electrum-rpc-url 0.0.0.0:50015 \
  --bitcoind-rpc-url http://127.0.0.1:8332 \
  --bitcoind-rpc-user admin \
  --bitcoind-rpc-pass admin \
  --bitcoind-blocks-dir "$HOME/.bitcoin/blocks"


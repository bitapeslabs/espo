
<img width="977" height="292" alt="espothumb (1)" src="https://github.com/user-attachments/assets/8ce11e4d-2fed-482a-bf26-1d520544ee7d" />

# Espo

### NOTE: 
A FREE version of ESPO is hosted at https://api.alkanode.com for anyone to use - courtesy of pizza.fun.

Espo is a production ready, general purpose indexer for Alkanes that builds and serves through its RPC indicies for highly sought after data may not be available through the default Sandshrew api. 

Espo does this through the concept of "modules" - during indexing, espo generates a struct called `EspoBlock`, which contains all the alkanes traces and transactions in a block. A pointer to espo block is passed around modules - in which they can then interpret as they wish to build any sort of indicies they like, such as OHLC data for example. 


 ### Requirements
 - A fully sycned Electrs (esplora fork): https://github.com/Blockstream/electrs
 - A fully sycned bitcoin core WITH txindex enabled
 - A fully synced metashrew


## Installation 
To start, clone the repo and build the binary:
```bash
git clone git@github.com:bitapeslabs/espo.git
cargo build --release
```

after the binary is built, you can run the indexer with the following command
```bash
./target/release/espo \
  --readonly-metashrew-db-dir /data/.metashrew/mainnet-v2.0.0/.metashrew-reconcile \
  --port {YOUR PORT} \
  --electrum-rpc-url {YOUR ELECTRUM ENDPOINT WITH PORT, NO HTTP:// PREFIX} \
  --bitcoind-rpc-url {YOUR BITCOIN RPC ENDPOINT WITH PORT, WITH HTTP:// PREFIX} \
  --bitcoind-rpc-user {BITCOIN RPC USERNAME} \
  --bitcoind-rpc-pass {BITCOIN RPC PASSWORD} \
  --bitcoind-blocks-dir {BITCOIN BLOCKS DIR}
```

Espo will build indicies for the .blk files in your bitcoin blocks directory and start indexing, with a fallback to the bitcoin RPC. I have only tested espo on my machine which has 32 cores adn 192gb of ram, and I achieve an index in a little less than 2 hours. On older hardware you can expect an index between 6-12 hours.

## Modules
- AMMDATA module (OHLC data, trades on oylswap, etc):
  https://github.com/bitapeslabs/espo/tree/main/src/modules/ammdata
  
- ESSENTIALS module (balances, holders data, address outpoints, K/V stores for contracts:
  https://github.com/bitapeslabs/espo/tree/main/src/modules/essentials

## Credits and License
This project is mantained by the pizza.fun foundation and opensourced to foster new developments on Alkanes. 

Espo is licensed under the BUSL agreement, which allows personal AND commercial use of the software UNLESS you are building a direct competitor to pizza.fun.



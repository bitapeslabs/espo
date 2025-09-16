use borsh::BorshDeserialize;
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};

use super::schemas::{SchemaCandleV1, SchemaTradeV1};
use crate::modules::defs::RpcNsRegistrar;
use crate::runtime::mdb::Mdb;

fn now_ts() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

#[allow(dead_code)]
pub fn register_rpc(reg: &RpcNsRegistrar, mdb: Mdb) {
    eprintln!("[RPC_AMMDATA] registering RPC handlers…");

    // GET TRADES
    //let mdb_trades = mdb.clone();
    //let reg_trades = reg.clone();
    tokio::spawn(async move {
        /*
        reg_trades
            .register("get_trades", move |_cx, payload| {

            })
            .await;
        */
    });

    // GET CANDLES
    //let mdb_candles = mdb.clone();
    //let reg_candles = reg.clone();
    tokio::spawn(async move {
        /*
        reg_candles
            .register("get_candles", move |_cx, payload| {

            })
            .await;
        */
    });

    let reg_ping = reg.clone();
    tokio::spawn(async move {
        reg_ping
            .register("ping", |_cx, _payload| async move { Value::String("pong".to_string()) })
            .await;
    });
    eprintln!("[RPC_AMMDATA] RPC handlers ready.");
}

use super::defs::SignedU128;
use super::lib::accumulate_alkane_balance_deltas;
use crate::alkanes::trace::{EspoHostFunctionValues, EspoSandshrewLikeTrace, get_espo_block};
use crate::config::{AppConfig, init_config_from};
use crate::core::blockfetcher::BlockFetchMode;
use crate::schemas::SchemaAlkaneId;
use bitcoin::{Txid, hashes::Hash};
use std::collections::HashMap;

fn init_test_config_from_run_sh() {
    let cfg = AppConfig {
        debug_backup: None,
        readonly_metashrew_db_dir: "/data/.metashrew/v9/.metashrew-v9".to_string(),
        electrum_rpc_url: None,
        metashrew_rpc_url: "http://127.0.0.1:7044".to_string(),
        electrs_esplora_url: Some("http://127.0.0.1:4332".to_string()),
        bitcoind_rpc_url: "http://127.0.0.1:8332".to_string(),
        bitcoind_rpc_user: "admin".to_string(),
        bitcoind_rpc_pass: "admin".to_string(),
        bitcoind_blocks_dir: "~/.bitcoin/blocks".to_string(),
        reset_mempool_on_startup: true,
        view_only: true,
        db_path: "./db".to_string(),
        sdb_poll_ms: 5000,
        indexer_block_delay_ms: 0,
        port: 5778,
        explorer_host: Some("0.0.0.0:5779".parse().expect("parse explorer_host")),
        explorer_base_path: "/".to_string(),
        explorer_pizza_tv_endpoint: "https://tv.pizza.fun".to_string(),
        network: bitcoin::Network::Bitcoin,
        metashrew_db_label: None,
        strict_mode: None,
        debug: false,
        debug_ignore_ms: 0,
        safe_tip_hook_script: None,
        block_source_mode: BlockFetchMode::RpcOnly,
        compact_tx_trace_rows: true,
        address_index_chunk_size: 512,
        explorer_networks: None,
        google_analytics_tag: None,
        modules: HashMap::new(),
    };
    if let Err(err) = init_config_from(cfg) {
        if !err.to_string().contains("already initialized") {
            panic!("init config from run.sh args: {err}");
        }
    }
}

fn test_host_function_values() -> EspoHostFunctionValues {
    // Block 909402 host function outputs (header, coinbase tx, total miner fee).
    let header_hex = "0060d52d0116257ed16bdf8429aea185252dee053b8c5e1b6137000000000000000000005c745239a3cdd49796f824a9e02059e482aac6916881351fabd1d957377cf6cde4699868b32c02170d1142fa";
    let coinbase_hex = "020000000001010000000000000000000000000000000000000000000000000000000000000000ffffffff5d035ae00d04e46998682f466f756e6472792055534120506f6f6c202364726f70676f6c642ffabe6d6d69933229fd402b4abc31138ebe7e0d29c73165583caee3e027c66614e70c1bb9010000000000000052a844004c6c010000000000ffffffff04e607c012000000002200207086320071974eef5e72eaa01dd9096e10c0383483855ea6b344259c244f73c20000000000000000266a24aa21a9ed29e8503898a86dab734385be38aba9f2477e4314dbde4833da2926f7c304fad100000000000000002f6a2d434f524501ba57b8de67e0cf289c1ee39f1f888767003819aae6d18fda214e5b9f350ffc7b6cf3058b9026e76500000000000000002b6a2952534b424c4f434b3a5319cc64514d1cc78f23bdd9313781e1284438ad2ad4818444e6e10e00781f6e0120000000000000000000000000000000000000000000000000000000000000000000000000";
    // Diesel mint count for this block is not yet wired in tests; placeholder.
    let diesel_hex = "00000000000000000000000000000000";
    let fee_hex = "e607c012000000000000000000000000";

    (
        hex::decode(header_hex).expect("decode block header hex"),
        hex::decode(coinbase_hex).expect("decode coinbase tx hex"),
        hex::decode(diesel_hex).expect("decode diesel mints hex"),
        hex::decode(fee_hex).expect("decode total miner fee hex"),
    )
}

#[test]
fn host_function_values_decode_block_909402() {
    let (header, coinbase, diesel, fee) = test_host_function_values();
    assert_eq!(header.len(), 80);
    assert!(!coinbase.is_empty());
    assert_eq!(diesel.len(), 16);
    assert_eq!(fee.len(), 16);
}

#[test]
#[ignore] // Requires external metashrew database at /data/.metashrew/v9/.metashrew-v9
fn credits_outflows_for_block_912568_trace() {
    init_test_config_from_run_sh();
    let height = 912568u64;
    let block = get_espo_block(height, height).expect("load espo block");
    let host_values = block.host_function_values;
    let trace_json = r#"
{
  "outpoint": "test",
  "events": [
  {
    "event": "invoke",
    "data": {
      "type": "call",
      "context": {
        "myself": {
          "block": "0x4",
          "tx": "0xfff2"
        },
        "caller": {
          "block": "0x0",
          "tx": "0x0"
        },
        "inputs": [
          "0xd",
          "0x2",
          "0x2",
          "0xc3f9",
          "0x2",
          "0x0",
          "0x28ed6103d000",
          "0x254d45ac",
          "0xdecb9",
          "0x0",
          "0x0",
          "0x0"
        ],
        "incomingAlkanes": [
          {
            "id": {
              "block": "0x2",
              "tx": "0xc3f9"
            },
            "value": "0x28ed6103d000"
          }
        ],
        "vout": 5
      },
      "fuel": 17881588
    }
  },
  {
    "event": "invoke",
    "data": {
      "type": "delegatecall",
      "context": {
        "myself": {
          "block": "0x4",
          "tx": "0xfff2"
        },
        "caller": {
          "block": "0x0",
          "tx": "0x0"
        },
        "inputs": [
          "0xd",
          "0x2",
          "0x2",
          "0xc3f9",
          "0x2",
          "0x0",
          "0x28ed6103d000",
          "0x254d45ac",
          "0xdecb9",
          "0x0",
          "0x0",
          "0x0"
        ],
        "incomingAlkanes": [
          {
            "id": {
              "block": "0x2",
              "tx": "0xc3f9"
            },
            "value": "0x28ed6103d000"
          }
        ],
        "vout": 5
      },
      "fuel": 17751703
    }
  },
  {
    "event": "invoke",
    "data": {
      "type": "call",
      "context": {
        "myself": {
          "block": "0x2",
          "tx": "0xf25c"
        },
        "caller": {
          "block": "0x4",
          "tx": "0xfff2"
        },
        "inputs": [
          "0x61"
        ],
        "incomingAlkanes": [],
        "vout": 5
      },
      "fuel": 17609962
    }
  },
  {
    "event": "invoke",
    "data": {
      "type": "staticcall",
      "context": {
        "myself": {
          "block": "0x4",
          "tx": "0xfff3"
        },
        "caller": {
          "block": "0x2",
          "tx": "0xf25c"
        },
        "inputs": [
          "0x7ffd"
        ],
        "incomingAlkanes": [],
        "vout": 5
      },
      "fuel": 17549141
    }
  },
  {
    "event": "return",
    "data": {
      "status": "success",
      "response": {
        "alkanes": [],
        "data": "0x04000000000000000000000000000000f0ff0000000000000000000000000000",
        "storage": []
      }
    }
  },
  {
    "event": "invoke",
    "data": {
      "type": "delegatecall",
      "context": {
        "myself": {
          "block": "0x2",
          "tx": "0xf25c"
        },
        "caller": {
          "block": "0x4",
          "tx": "0xfff2"
        },
        "inputs": [
          "0x61"
        ],
        "incomingAlkanes": [],
        "vout": 5
      },
      "fuel": 17472430
    }
  },
  {
    "event": "return",
    "data": {
      "status": "success",
      "response": {
        "alkanes": [],
        "data": "0x5663f2cc03000000000000000000000016248da4738e03000000000000000000",
        "storage": []
      }
    }
  },
  {
    "event": "return",
    "data": {
      "status": "success",
      "response": {
        "alkanes": [],
        "data": "0x5663f2cc03000000000000000000000016248da4738e03000000000000000000",
        "storage": []
      }
    }
  },
  {
    "event": "invoke",
    "data": {
      "type": "call",
      "context": {
        "myself": {
          "block": "0x2",
          "tx": "0xf25c"
        },
        "caller": {
          "block": "0x4",
          "tx": "0xfff2"
        },
        "inputs": [
          "0x3",
          "0x29a79f03",
          "0x0",
          "0x0",
          "0x0",
          "0x0"
        ],
        "incomingAlkanes": [
          {
            "id": {
              "block": "0x2",
              "tx": "0xc3f9"
            },
            "value": "0x28ed6103d000"
          }
        ],
        "vout": 5
      },
      "fuel": 17234315
    }
  },
  {
    "event": "invoke",
    "data": {
      "type": "staticcall",
      "context": {
        "myself": {
          "block": "0x4",
          "tx": "0xfff3"
        },
        "caller": {
          "block": "0x2",
          "tx": "0xf25c"
        },
        "inputs": [
          "0x7ffd"
        ],
        "incomingAlkanes": [],
        "vout": 5
      },
      "fuel": 17150646
    }
  },
  {
    "event": "return",
    "data": {
      "status": "success",
      "response": {
        "alkanes": [],
        "data": "0x04000000000000000000000000000000f0ff0000000000000000000000000000",
        "storage": []
      }
    }
  },
  {
    "event": "invoke",
    "data": {
      "type": "delegatecall",
      "context": {
        "myself": {
          "block": "0x2",
          "tx": "0xf25c"
        },
        "caller": {
          "block": "0x4",
          "tx": "0xfff2"
        },
        "inputs": [
          "0x3",
          "0x29a79f03",
          "0x0",
          "0x0",
          "0x0",
          "0x0"
        ],
        "incomingAlkanes": [
          {
            "id": {
              "block": "0x2",
              "tx": "0xc3f9"
            },
            "value": "0x28ed6103d000"
          }
        ],
        "vout": 5
      },
      "fuel": 17053982
    }
  },
  {
    "event": "return",
    "data": {
      "status": "success",
      "response": {
        "alkanes": [],
        "data": "0x00008020da6c14e4337181727e42469716b667ad2759d10b9a0001000000000000000000a0e476bb18f70a0334948d6c304e2bc63df2a96d2f93bcf69e6b9caca11f92bda86ab468912b02171fd51120",
        "storage": []
      }
    }
  },
  {
    "event": "return",
    "data": {
      "status": "success",
      "response": {
        "alkanes": [
          {
            "id": {
              "block": "0x2",
              "tx": "0x0"
            },
            "value": "0x29a79f03"
          }
        ],
        "data": "0x",
        "storage": [
          {
            "key": "/blockTimestampLast",
            "value": "0xa86ab468"
          },
          {
            "key": "/lock",
            "value": "0x00000000000000000000000000000000"
          },
          {
            "key": "/price0CumLast",
            "value": "0xd668a51ea31c07fddccee5114d77974ef77b34d95c0000000000000000000000"
          },
          {
            "key": "/price1CumLast",
            "value": "0x221b0d96fe22ac663687ee61184260c820000000000000000000000000000000"
          }
        ]
      }
    }
  },
  {
    "event": "return",
    "data": {
      "status": "success",
      "response": {
        "alkanes": [
          {
            "id": {
              "block": "0x2",
              "tx": "0x0"
            },
            "value": "0x29a79f03"
          }
        ],
        "data": "0x",
        "storage": []
      }
    }
  },
  {
    "event": "return",
    "data": {
      "status": "success",
      "response": {
        "alkanes": [
          {
            "id": {
              "block": "0x2",
              "tx": "0x0"
            },
            "value": "0x29a79f03"
          },
          {
            "id": {
              "block": "0x2",
              "tx": "0xc3f9"
            },
            "value": "0x0"
          }
        ],
        "data": "0x",
        "storage": []
      }
    }
  },
  {
    "event": "return",
    "data": {
      "status": "success",
      "response": {
        "alkanes": [
          {
            "id": {
              "block": "0x2",
              "tx": "0x0"
            },
            "value": "0x29a79f03"
          },
          {
            "id": {
              "block": "0x2",
              "tx": "0xc3f9"
            },
            "value": "0x0"
          }
        ],
        "data": "0x",
        "storage": []
      }
    }
  }
  ]
}
"#;

    let trace: EspoSandshrewLikeTrace = serde_json::from_str(trace_json).expect("parse trace");
    let (ok, deltas) =
        accumulate_alkane_balance_deltas(&trace, &Txid::from_byte_array([0u8; 32]), &host_values);
    assert!(ok);

    let owner = SchemaAlkaneId { block: 2, tx: 0xf25c };
    let token_native = SchemaAlkaneId { block: 2, tx: 0x0 };
    let token_in = SchemaAlkaneId { block: 2, tx: 0xc3f9 };

    let owner_out = deltas.get(&owner).cloned().unwrap_or_default();
    assert_eq!(owner_out.get(&token_native).copied(), Some(SignedU128::negative(698_851_075)));
    assert_eq!(owner_out.get(&token_in).copied(), Some(SignedU128::positive(45_000_000_000_000)));
}

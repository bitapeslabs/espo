use axum::response::Html;
use bitcoincore_rpc::RpcApi;
use maud::html;

use crate::config::{get_bitcoind_rpc_client, get_espo_next_height};
use crate::explorer::components::layout::layout;

pub async fn home_page() -> Html<String> {
    let rpc = get_bitcoind_rpc_client();
    let tip = rpc.get_blockchain_info().map(|i| i.blocks).unwrap_or(0);
    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let latest_height = espo_tip.min(tip);

    let mut blocks: Vec<(u64, String, Option<u64>, Option<usize>)> = Vec::new();
    let n: u64 = 15;
    for i in 0..n {
        if latest_height < i {
            break;
        }
        let height = latest_height.saturating_sub(i);
        let hash = match rpc.get_block_hash(height) {
            Ok(h) => h,
            Err(_) => continue,
        };
        let hdr = rpc.get_block_header_info(&hash).ok();
        let time = hdr.as_ref().map(|h| h.time as u64);
        let txs = hdr.as_ref().map(|h| h.n_tx);
        blocks.push((height, hash.to_string(), time, txs));
    }

    layout(
        "Blocks",
        html! {
            div class="row" {
                h1 class="h1" { "Blocks" }
                form class="search" method="get" action="/search" {
                    input class="input" type="text" name="q" placeholder="Search height or block hash";
                    button class="btn" type="submit" { "Search" }
                }
            }

            div class="card" {
                table class="table" {
                    thead {
                        tr {
                            th { "Height" }
                            th { "Hash" }
                            th { "Time" }
                            th class="right" { "Txs" }
                        }
                    }
                    tbody {
                        @for (height, hash, time, txs) in blocks {
                            tr {
                                td { a class="link" href=(format!("/block/{height}")) { (height) } }
                                td class="mono" { (hash) }
                                td class="mono" {
                                    @match time {
                                        Some(t) => { (t) }
                                        None => { span class="muted" { "—" } }
                                    }
                                }
                                td class="right" {
                                    @match txs {
                                        Some(n) => { (n) }
                                        None => { span class="muted" { "—" } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
    )
}

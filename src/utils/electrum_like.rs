use anyhow::{Context, Result};
use bitcoin::{Address, Txid};
use electrum_client::{Client as ElectrumClient, ElectrumApi, Param};
use futures::{StreamExt, stream::FuturesUnordered};
use reqwest::Client as HttpClient;
use serde::Deserialize;
use std::collections::HashSet;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ElectrumLikeBackend {
    ElectrumRpc,
    EsploraHttp,
}

#[derive(Clone, Debug)]
pub struct AddressStats {
    pub backend: ElectrumLikeBackend,
    pub confirmed_balance: Option<u64>,
    pub total_received: Option<u64>,
    pub confirmed_utxos: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct AddressHistoryEntry {
    pub txid: Txid,
    pub height: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct AddressHistoryPage {
    pub entries: Vec<AddressHistoryEntry>,
    pub total: Option<usize>,
    pub has_more: bool,
}

/// Minimal interface needed by ammdata for fetching raw transactions.
pub trait ElectrumLike: Send + Sync {
    fn batch_transaction_get_raw(&self, txids: &[Txid]) -> Result<Vec<Vec<u8>>>;
    fn transaction_get_raw(&self, txid: &Txid) -> Result<Vec<u8>>;
    fn tip_height(&self) -> Result<u32>;
    fn transaction_get_outspends(&self, txid: &Txid) -> Result<Vec<Option<Txid>>>;
    fn batch_transaction_get_outspends(&self, txids: &[Txid]) -> Result<Vec<Vec<Option<Txid>>>>;
    fn transaction_get_height(&self, txid: &Txid) -> Result<Option<u64>>;
    fn address_stats(&self, address: &Address) -> Result<AddressStats>;
    fn address_history_page(
        &self,
        address: &Address,
        offset: usize,
        limit: usize,
    ) -> Result<AddressHistoryPage>;
}

/// Thin wrapper over the native Electrum RPC client.
pub struct ElectrumRpcClient {
    client: Arc<ElectrumClient>,
}

impl ElectrumRpcClient {
    pub fn new(client: Arc<ElectrumClient>) -> Self {
        Self { client }
    }
}

impl ElectrumLike for ElectrumRpcClient {
    fn batch_transaction_get_raw(&self, txids: &[Txid]) -> Result<Vec<Vec<u8>>> {
        self.client
            .batch_transaction_get_raw(txids)
            .context("electrum batch_transaction_get_raw")
    }

    fn transaction_get_raw(&self, txid: &Txid) -> Result<Vec<u8>> {
        self.client.transaction_get_raw(txid).context("electrum transaction_get_raw")
    }

    fn tip_height(&self) -> Result<u32> {
        let sub = self
            .client
            .block_headers_subscribe_raw()
            .context("electrum: blockchain.headers.subscribe failed")?;

        let tip: u32 = sub
            .height
            .try_into()
            .map_err(|_| anyhow::anyhow!("electrum: tip height doesn't fit into u32"))?;
        Ok(tip)
    }

    fn transaction_get_outspends(&self, txid: &Txid) -> Result<Vec<Option<Txid>>> {
        // Electrum protocol does not standardize outspends; try verbose get (electrs supports
        // `outspends` field) and fall back to empty.
        let params = vec![Param::String(txid.to_string()), Param::Bool(true)];
        let resp = self
            .client
            .raw_call("blockchain.transaction.get", params)
            .context("electrum transaction.get (verbose) for outspends")?;

        Ok(extract_outspends(&resp))
    }

    fn batch_transaction_get_outspends(&self, txids: &[Txid]) -> Result<Vec<Vec<Option<Txid>>>> {
        let mut out = Vec::with_capacity(txids.len());
        for txid in txids {
            let res = self.transaction_get_outspends(txid).unwrap_or_default();
            out.push(res);
        }
        Ok(out)
    }

    fn transaction_get_height(&self, txid: &Txid) -> Result<Option<u64>> {
        let params = vec![Param::String(txid.to_string()), Param::Bool(true)];
        let resp = self
            .client
            .raw_call("blockchain.transaction.get", params)
            .context("electrum transaction.get (verbose) for height")?;

        Ok(extract_height(&resp))
    }

    fn address_stats(&self, address: &Address) -> Result<AddressStats> {
        let script = address.script_pubkey();
        let balance =
            self.client.script_get_balance(&script).context("electrum script_get_balance")?;
        let utxos = self
            .client
            .script_list_unspent(&script)
            .context("electrum script_list_unspent")?;
        let confirmed_utxos = utxos.into_iter().filter(|u| u.height > 0).count();

        Ok(AddressStats {
            backend: ElectrumLikeBackend::ElectrumRpc,
            confirmed_balance: Some(balance.confirmed),
            total_received: None,
            confirmed_utxos: Some(confirmed_utxos),
        })
    }

    fn address_history_page(
        &self,
        address: &Address,
        offset: usize,
        limit: usize,
    ) -> Result<AddressHistoryPage> {
        let script = address.script_pubkey();
        let history =
            self.client.script_get_history(&script).context("electrum script_get_history")?;
        let total = history.len();
        let slice = history.into_iter().skip(offset).take(limit);
        let mut out = Vec::new();
        for h in slice {
            let height = if h.height > 0 { Some(h.height as u64) } else { None };
            out.push(AddressHistoryEntry { txid: h.tx_hash, height });
        }
        Ok(AddressHistoryPage {
            entries: out,
            total: Some(total),
            has_more: offset + limit < total,
        })
    }
}

/// Esplora-backed implementation that hits `/tx/:txid/raw`.
pub struct EsploraElectrumLike {
    base_url: String,
    http: HttpClient,
}

impl EsploraElectrumLike {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let mut url = base_url.into();
        while url.ends_with('/') {
            url.pop();
        }
        Ok(Self { base_url: url, http: HttpClient::new() })
    }

    async fn fetch_one_indexed(&self, idx: usize, txid: &Txid) -> Result<(usize, Vec<u8>)> {
        let url = format!("{}/tx/{}/raw", self.base_url, txid);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("esplora GET {url} failed"))?
            .error_for_status()
            .with_context(|| format!("esplora GET {url} returned error status"))?;

        let bytes = resp.bytes().await.context("esplora response body read failed")?;
        Ok((idx, bytes.to_vec()))
    }

    fn block_on_result<F, T>(&self, fut: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        match Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Err(_) => {
                let rt = Runtime::new().context("failed to build ad-hoc Tokio runtime")?;
                rt.block_on(fut)
            }
        }
    }

    async fn fetch_address_summary(&self, address: &str) -> Result<EsploraAddressSummary> {
        let url = format!("{}/address/{}", self.base_url, address);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("esplora GET {url} failed"))?
            .error_for_status()
            .with_context(|| format!("esplora GET {url} returned error status"))?;

        let body_str = resp.text().await.context("esplora address body read failed")?;
        let summary: EsploraAddressSummary =
            serde_json::from_str(&body_str).context("esplora address json decode failed")?;
        Ok(summary)
    }

    async fn fetch_address_utxos(&self, address: &str) -> Result<Vec<EsploraUtxo>> {
        let url = format!("{}/address/{}/utxo", self.base_url, address);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("esplora GET {url} failed"))?
            .error_for_status()
            .with_context(|| format!("esplora GET {url} returned error status"))?;

        let body_str = resp.text().await.context("esplora utxo body read failed")?;
        let utxos: Vec<EsploraUtxo> =
            serde_json::from_str(&body_str).context("esplora utxo json decode failed")?;
        Ok(utxos)
    }

    async fn fetch_outspends_single(&self, txid: &Txid) -> Result<Vec<Option<Txid>>> {
        let url = format!("{}/tx/{}/outspends", self.base_url, txid);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("esplora GET {url} failed"))?
            .error_for_status()
            .with_context(|| format!("esplora GET {url} returned error status"))?;

        let body_str = resp.text().await.context("esplora outspends body read failed")?;
        let body: Vec<EsploraOutspend> =
            serde_json::from_str(&body_str).context("esplora outspends json decode failed")?;

        let mut out = Vec::with_capacity(body.len());
        for o in body {
            if o.spent {
                out.push(o.txid.and_then(|s| Txid::from_str(&s).ok()));
            } else {
                out.push(None);
            }
        }
        Ok(out)
    }

    async fn fetch_outspends_batch(&self, txids: &[Txid]) -> Vec<Vec<Option<Txid>>> {
        let futs = txids.iter().enumerate().map(|(idx, txid)| async move {
            (idx, txid, self.fetch_outspends_single(txid).await)
        });
        let mut out = vec![Vec::new(); txids.len()];
        for (idx, txid, res) in futures::future::join_all(futs).await {
            match res {
                Ok(v) => out[idx] = v,
                Err(e) => {
                    eprintln!("[esplora] failed to fetch outspends for {txid}: {e:?}");
                    out[idx] = Vec::new();
                }
            }
        }
        out
    }
    async fn fetch_address_txs(&self, url: &str) -> Result<Vec<EsploraAddressTx>> {
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .with_context(|| format!("esplora GET {url} failed"))?
            .error_for_status()
            .with_context(|| format!("esplora GET {url} returned error status"))?;

        let body_str = resp.text().await.context("esplora address txs body read failed")?;
        let txs: Vec<EsploraAddressTx> =
            serde_json::from_str(&body_str).context("esplora address txs json decode failed")?;
        Ok(txs)
    }
}

impl ElectrumLike for EsploraElectrumLike {
    fn batch_transaction_get_raw(&self, txids: &[Txid]) -> Result<Vec<Vec<u8>>> {
        if txids.is_empty() {
            return Ok(Vec::new());
        }

        self.block_on_result(async {
            let mut futs = FuturesUnordered::new();
            for (idx, txid) in txids.iter().enumerate() {
                futs.push(self.fetch_one_indexed(idx, txid));
            }

            let mut out = vec![Vec::new(); txids.len()];
            while let Some(res) = futs.next().await {
                match res {
                    Ok((idx, raw)) => out[idx] = raw,
                    Err(e) => eprintln!("[esplora] failed to fetch raw tx: {e:?}"),
                }
            }
            Ok(out)
        })
    }

    fn transaction_get_raw(&self, txid: &Txid) -> Result<Vec<u8>> {
        self.block_on_result(async {
            let (_, raw) = self.fetch_one_indexed(0, txid).await?;
            Ok(raw)
        })
    }

    fn tip_height(&self) -> Result<u32> {
        self.block_on_result(async {
            let url = format!("{}/blocks/tip/height", self.base_url);
            let resp = self
                .http
                .get(&url)
                .send()
                .await
                .with_context(|| format!("esplora GET {url} failed"))?
                .error_for_status()
                .with_context(|| format!("esplora GET {url} returned error status"))?;

            let body = resp.text().await.context("esplora tip height body read failed")?;
            let tip: u32 = body
                .trim()
                .parse()
                .with_context(|| format!("failed to parse esplora tip height from '{body}'"))?;
            Ok(tip)
        })
    }

    fn transaction_get_outspends(&self, txid: &Txid) -> Result<Vec<Option<Txid>>> {
        self.block_on_result(async { self.fetch_outspends_single(txid).await })
    }

    fn batch_transaction_get_outspends(&self, txids: &[Txid]) -> Result<Vec<Vec<Option<Txid>>>> {
        if txids.is_empty() {
            return Ok(Vec::new());
        }
        self.block_on_result(async { Ok(self.fetch_outspends_batch(txids).await) })
    }

    fn transaction_get_height(&self, txid: &Txid) -> Result<Option<u64>> {
        self.block_on_result(async {
            let url = format!("{}/tx/{}", self.base_url, txid);
            let resp = self
                .http
                .get(&url)
                .send()
                .await
                .with_context(|| format!("esplora GET {url} failed"))?
                .error_for_status()
                .with_context(|| format!("esplora GET {url} returned error status"))?;

            let body_str = resp.text().await.context("esplora tx body read failed")?;
            let body: serde_json::Value =
                serde_json::from_str(&body_str).context("esplora tx json decode failed")?;

            let height =
                body.get("status").and_then(|s| s.get("block_height")).and_then(|h| h.as_u64());
            Ok(height)
        })
    }

    fn address_stats(&self, address: &Address) -> Result<AddressStats> {
        let addr = address.to_string();
        self.block_on_result(async {
            let summary = self.fetch_address_summary(&addr).await?;
            let utxos = self.fetch_address_utxos(&addr).await?;

            let confirmed_balance = summary
                .chain_stats
                .funded_txo_sum
                .saturating_sub(summary.chain_stats.spent_txo_sum);
            let confirmed_utxos = utxos.into_iter().filter(|u| u.status.confirmed).count();

            Ok(AddressStats {
                backend: ElectrumLikeBackend::EsploraHttp,
                confirmed_balance: Some(confirmed_balance),
                total_received: Some(summary.chain_stats.funded_txo_sum),
                confirmed_utxos: Some(confirmed_utxos),
            })
        })
    }

    fn address_history_page(
        &self,
        address: &Address,
        offset: usize,
        limit: usize,
    ) -> Result<AddressHistoryPage> {
        let addr = address.to_string();
        self.block_on_result(async {
            let mut out: Vec<AddressHistoryEntry> = Vec::new();
            let mut seen: HashSet<Txid> = HashSet::new();
            let mut last_seen: Option<String> = None;
            let mut consumed: usize = 0;
            let page_size: usize = 25;

            // Best-effort total via summary.
            let summary = self.fetch_address_summary(&addr).await.ok();
            let total = summary.as_ref().map(|s| s.chain_stats.tx_count as usize);

            while out.len() < limit {
                let url = match last_seen.as_ref() {
                    Some(txid) => format!("{}/address/{}/txs/chain/{}", self.base_url, addr, txid),
                    None => format!("{}/address/{}/txs", self.base_url, addr),
                };
                let page = self.fetch_address_txs(&url).await?;
                if page.is_empty() {
                    break;
                }

                let page_len = page.len();
                // Skip until we reach the requested offset.
                if consumed + page_len <= offset {
                    consumed += page_len;
                    last_seen = page.last().map(|t| t.txid.clone());
                    continue;
                }

                let start_idx = offset.saturating_sub(consumed);
                for tx in page.iter().skip(start_idx) {
                    if let Ok(txid) = Txid::from_str(&tx.txid) {
                        if seen.insert(txid) {
                            out.push(AddressHistoryEntry { txid, height: tx.status.block_height });
                            if out.len() >= limit {
                                break;
                            }
                        }
                    }
                }

                consumed += page_len;
                let next = page.last().map(|t| t.txid.clone());
                if next.is_none() || next == last_seen {
                    break;
                }
                last_seen = next;

                if page_len < page_size {
                    break;
                }
            }

            let has_more = if let Some(total) = total {
                offset + out.len() < total
            } else {
                out.len() == limit
            };

            Ok(AddressHistoryPage { entries: out, total, has_more })
        })
    }
}

fn extract_height(resp: &serde_json::Value) -> Option<u64> {
    resp.get("height")
        .or_else(|| resp.get("blockheight"))
        .or_else(|| resp.get("block_height"))
        .and_then(|h| h.as_i64())
        .and_then(|h| if h >= 0 { Some(h as u64) } else { None })
}

#[derive(Deserialize)]
struct EsploraAddressSummary {
    chain_stats: EsploraAddressChainStats,
}

#[derive(Deserialize)]
struct EsploraAddressChainStats {
    funded_txo_sum: u64,
    spent_txo_sum: u64,
    tx_count: u64,
}

#[derive(Deserialize)]
struct EsploraUtxo {
    status: EsploraUtxoStatus,
}

#[derive(Deserialize)]
struct EsploraUtxoStatus {
    confirmed: bool,
}

#[derive(Deserialize)]
struct EsploraOutspend {
    spent: bool,
    txid: Option<String>,
}

#[derive(Deserialize)]
struct EsploraAddressTx {
    txid: String,
    status: EsploraAddressTxStatus,
}

#[derive(Deserialize)]
struct EsploraAddressTxStatus {
    #[allow(dead_code)]
    confirmed: bool,
    block_height: Option<u64>,
}

fn extract_outspends(v: &serde_json::Value) -> Vec<Option<Txid>> {
    let Some(arr) = v.get("outspends").and_then(|o| o.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let spent_flag = item.get("spent").and_then(|b| b.as_bool());
        let spender =
            item.get("txid").and_then(|t| t.as_str()).and_then(|s| Txid::from_str(s).ok());
        let spent = spent_flag.unwrap_or_else(|| spender.is_some());
        out.push(if spent { spender } else { None });
    }
    out
}

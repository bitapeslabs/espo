use anyhow::{Context, Result};
use bitcoin::Txid;
use electrum_client::{Client as ElectrumClient, ElectrumApi};
use futures::{stream::FuturesUnordered, StreamExt};
use reqwest::Client as HttpClient;
use std::future::Future;
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};

/// Minimal interface needed by ammdata for fetching raw transactions.
pub trait ElectrumLike: Send + Sync {
    fn batch_transaction_get_raw(&self, txids: &[Txid]) -> Result<Vec<Vec<u8>>>;
    fn transaction_get_raw(&self, txid: &Txid) -> Result<Vec<u8>>;
    fn tip_height(&self) -> Result<u32>;
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
        self.client
            .transaction_get_raw(txid)
            .context("electrum transaction_get_raw")
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
}

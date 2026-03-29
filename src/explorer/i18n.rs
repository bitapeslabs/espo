#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExplorerLanguage {
    English,
    Chinese,
}

impl ExplorerLanguage {
    pub fn is_chinese(self) -> bool {
        matches!(self, Self::Chinese)
    }
}

const EN_TO_ZH: &[(&str, &str)] = &[
    ("Search blocks, alkanes, transactions, addresses", "搜索区块、Alkane、交易、地址"),
    (
        "Providing inputs to write methods is not currently supported on Espo",
        "Espo 目前不支持为写方法提供输入",
    ),
    (
        "Providing inputs to simulate methods is not currently supported on espo",
        "espo 目前不支持为模拟方法提供输入",
    ),
    ("Transactions will appear once ESPO indexes this block.", "ESPO 索引该区块后将显示交易。"),
    ("Address has too many transactions to render quickly.", "该地址交易过多，无法快速渲染。"),
    ("No transactions match the current filters.", "没有交易匹配当前筛选条件。"),
    ("Design inspired by ", "设计灵感来自 "),
    ("Created with ", "由 "),
    ("View more Alkanes", "查看更多 Alkanes"),
    ("View more Alkane txs", "查看更多 Alkane 交易"),
    ("Only Alkanes txs", "仅显示 Alkanes 交易"),
    ("No chart data for this selection", "当前选择没有图表数据"),
    ("Loading chart...", "图表加载中..."),
    ("Chart unavailable", "图表不可用"),
    ("Loading blocks…", "区块加载中…"),
    ("Network", "网络"),
    ("Mainnet", "主网"),
    ("Signet", "签名网"),
    ("Testnet3", "测试网3"),
    ("Testnet4", "测试网4"),
    ("Regtest", "回归测试网"),
    ("Address Type", "地址类型"),
    ("Confirmed UTXOs", "已确认 UTXO"),
    ("Confirmed balance", "已确认余额"),
    ("Order direction", "排序方向"),
    ("Order alkanes", "排序 Alkanes"),
    ("No alkanes tracked for this address.", "该地址暂无已跟踪的 Alkanes。"),
    ("No alkanes tracked for this alkane.", "该 Alkane 暂无已跟踪数据。"),
    ("No alkane transactions found.", "未找到 Alkane 交易。"),
    ("No transactions found.", "未找到交易。"),
    ("No contract methods found.", "未找到合约方法。"),
    ("No read methods.", "无读方法。"),
    ("No write methods.", "无写方法。"),
    ("No activity yet.", "暂无活动。"),
    ("No holders.", "暂无持有者。"),
    ("No alkanes found.", "未找到 Alkanes。"),
    ("Invalid address for this network.", "该地址不属于当前网络。"),
    ("Addresses", "地址"),
    ("Address", "地址"),
    ("Alkane Balances", "Alkane 余额"),
    ("Balance History", "余额历史"),
    ("Transactions", "交易"),
    ("Transfer Volume", "转账量"),
    ("Transfer volume", "转账量"),
    ("Total Received", "总接收量"),
    ("Total received", "总接收量"),
    ("Inspect contract", "合约检查"),
    ("View as block:", "按区块查看:"),
    ("Read methods:", "读方法:"),
    ("Write methods:", "写方法:"),
    ("Result:", "结果:"),
    ("Simulate anyways", "仍然模拟"),
    ("Hide Diesel mints", "隐藏 Diesel 铸造"),
    ("Latest Traces", "最新跟踪"),
    ("Latest blocks:", "最新区块:"),
    ("Top Alkanes", "热门 Alkanes"),
    ("Explore ", "探索 "),
    (" Bitcoin", " 比特币"),
    ("programmable", "可编程"),
    ("Market chart", "市场图表"),
    ("Market", "市场"),
    ("Overview", "概览"),
    ("Symbol", "符号"),
    ("Circulating supply", "流通供应量"),
    ("(with 8 decimals)", "(8 位小数)"),
    ("Holders", "持有者"),
    ("Holding %", "持仓占比"),
    ("Deploy date", "部署日期"),
    ("Deploy transaction", "部署交易"),
    ("Deploy block", "部署区块"),
    ("Unknown", "未知"),
    ("Showing ", "显示 "),
    ("Previous page", "上一页"),
    ("Next page", "下一页"),
    ("First page", "第一页"),
    ("Last page", "最后一页"),
    ("Unconfirmed", "未确认"),
    ("Pending", "待处理"),
    ("Blocks", "区块"),
    ("Block ", "区块 "),
    ("Alkanes", "Alkanes"),
    ("Search", "搜索"),
    ("Open menu", "打开菜单"),
    ("No inputs", "无输入"),
    ("No outputs", "无输出"),
    ("Inputs & Outputs", "输入与输出"),
    ("Inputs", "输入"),
    ("Outputs", "输出"),
    ("Fee rate", "费率"),
    ("Fee", "手续费"),
    ("Timestamp", "时间戳"),
    ("Tx count", "交易数量"),
    ("Address Type", "地址类型"),
    ("Creation block", "创建区块"),
    ("Creation tx", "创建交易"),
    ("Holder Count", "持有者数量"),
    ("Order by:", "排序方式:"),
    ("All Alkanes", "全部 Alkanes"),
    ("Descending", "降序"),
    ("Ascending", "升序"),
    ("Age", "年龄"),
    ("Balance", "余额"),
    ("Holder", "持有者"),
    ("Overview", "概览"),
    ("TOKEN", "代币"),
    ("Coinbase input", "Coinbase 输入"),
    ("Coinbase", "Coinbase"),
    ("Spent by transaction", "被该交易花费"),
    ("View previous transaction", "查看上一笔交易"),
    ("view on mempool.space", "在 mempool.space 查看"),
    ("GitHub repository", "GitHub 仓库"),
    ("Copy id", "复制 ID"),
    ("Contract call:", "合约调用："),
    ("Call successful", "调用成功"),
    ("Call reverted", "调用已回滚"),
    ("Reverted: ", "已回滚："),
    ("contract call", "合约调用"),
    ("factory clone", "工厂克隆"),
    ("unknown", "未知"),
    ("prevout unavailable", "前序输出不可用"),
    ("non-standard", "非标准"),
    ("Unspent output", "未花费输出"),
    ("OP_RETURN", "OP_RETURN"),
    ("Protostone message", "Protostone 消息"),
    (" Protostone message)", " Protostone 消息)"),
    ("Alkanes Trace #", "Alkanes 跟踪 #"),
    ("opcode ", "操作码 "),
    ("just now", "刚刚"),
    (" y ago", " 年前"),
    ("mo ago", " 个月前"),
    ("d ago", " 天前"),
    ("h ago", " 小时前"),
    ("m ago", " 分钟前"),
    (" confirmation", " 次确认"),
    (" confirmations", " 次确认"),
];

pub fn translate_html(language: ExplorerLanguage, html: String) -> String {
    if !language.is_chinese() {
        return html;
    }
    translate_html_excluding_code_blocks(&html)
}

fn translate_chunk(mut chunk: String) -> String {
    for (en, zh) in EN_TO_ZH {
        chunk = chunk.replace(en, zh);
    }
    chunk
}

fn find_next_tag(haystack: &str, from: usize, tag: &str) -> Option<usize> {
    haystack[from..].find(tag).map(|idx| from + idx)
}

fn translate_html_excluding_code_blocks(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut cursor = 0usize;

    while cursor < html.len() {
        let next_script = find_next_tag(html, cursor, "<script");
        let next_style = find_next_tag(html, cursor, "<style");

        let (block_start, end_tag) = match (next_script, next_style) {
            (Some(s), Some(st)) if s <= st => (s, "</script>"),
            (Some(s), Some(st)) if st < s => (st, "</style>"),
            (Some(s), None) => (s, "</script>"),
            (None, Some(st)) => (st, "</style>"),
            (None, None) => {
                out.push_str(&translate_chunk(html[cursor..].to_string()));
                break;
            }
            _ => unreachable!(),
        };

        out.push_str(&translate_chunk(html[cursor..block_start].to_string()));

        if let Some(end_rel) = html[block_start..].find(end_tag) {
            let block_end = block_start + end_rel + end_tag.len();
            out.push_str(&html[block_start..block_end]);
            cursor = block_end;
        } else {
            out.push_str(&html[block_start..]);
            break;
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::{ExplorerLanguage, translate_html};

    #[test]
    fn chinese_translation_does_not_mutate_script_contents() {
        let html = r#"<div>Search</div><script>const p = new URLSearchParams();</script>"#;
        let out = translate_html(ExplorerLanguage::Chinese, html.to_string());
        assert!(out.contains("<div>搜索</div>"));
        assert!(out.contains("URLSearchParams"));
        assert!(!out.contains("URL搜索Params"));
    }

    #[test]
    fn chinese_translation_still_translates_normal_markup() {
        let html = r#"<span>40 confirmations</span>"#;
        let out = translate_html(ExplorerLanguage::Chinese, html.to_string());
        assert!(out.contains("40 次确认"));
    }
}

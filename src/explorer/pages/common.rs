use bitcoin::Amount;

pub const ALKANE_SCALE: u128 = 100_000_000;

pub fn fmt_sats(sats: u64) -> String {
    let btc = Amount::from_sat(sats).to_btc();
    format!("{btc:.8} BTC")
}

pub fn fmt_amount(amount: Amount) -> String {
    fmt_sats(amount.to_sat())
}

pub fn fmt_alkane_amount(raw: u128) -> String {
    let whole = raw / ALKANE_SCALE;
    let frac = (raw % ALKANE_SCALE) as u64;
    if frac == 0 {
        return whole.to_string();
    }
    format!("{}.{}", whole, format!("{frac:08}"))
}

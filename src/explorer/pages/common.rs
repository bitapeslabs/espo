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
    fn with_commas(n: u128) -> String {
        let mut s = n.to_string();
        let mut i = s.len() as isize - 3;
        while i > 0 {
            s.insert(i as usize, ',');
            i -= 3;
        }
        s
    }

    let whole = raw / ALKANE_SCALE;
    let frac = (raw % ALKANE_SCALE) as u64;
    if frac == 0 {
        return with_commas(whole);
    }
    format!("{}.{}", with_commas(whole), format!("{frac:08}"))
}

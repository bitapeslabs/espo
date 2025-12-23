use bitcoin::Amount;

pub const ALKANE_SCALE: u128 = 100_000_000;

pub fn fmt_sats(sats: u64) -> String {
    const SATS_PER_BTC: u64 = 100_000_000;

    let whole = sats / SATS_PER_BTC;
    let frac = sats % SATS_PER_BTC;
    if frac == 0 {
        return format!("{whole} BTC");
    }

    let frac = trim_fraction(format!("{frac:08}"));
    format!("{whole}.{frac} BTC")
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

    let frac = trim_fraction(format!("{frac:08}"));
    format!("{}.{}", with_commas(whole), frac)
}

fn trim_fraction(mut s: String) -> String {
    while s.ends_with('0') {
        s.pop();
    }
    s
}

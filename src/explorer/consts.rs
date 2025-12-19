pub const DEFAULT_PAGE_LIMIT: usize = 25;
pub const MAX_PAGE_LIMIT: usize = 200;

pub const ALKANE_ICON_BASE: &str = "https://ordiscan.com/alkane";

/// Static overrides for alkanes missing metadata.
pub const ALKANE_NAME_OVERRIDES: &[(&str, &str, &str)] =
    &[("2:0", "DIESEL", "DIESEL"), ("32:0", "frBTC", "FRBTC")];

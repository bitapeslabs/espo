use crate::modules::ammdata::consts::KEY_INDEX_HEIGHT;
use crate::modules::ammdata::main::AmmData;
impl AmmData {
    pub fn get_key_index_height() -> &'static [u8] {
        KEY_INDEX_HEIGHT
    }
}

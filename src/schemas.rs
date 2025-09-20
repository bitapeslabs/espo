use borsh::{BorshDeserialize, BorshSerialize};

#[derive(
    BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone, Copy, Eq, PartialOrd, Ord, Hash,
)]
pub struct SchemaAlkaneId {
    pub block: u32,
    pub tx: u64,
}

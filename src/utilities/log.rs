use crate::utilities::transaction::Transaction;

pub struct Log {
    current_term : i32,
    log_index : i32,
    transaction : Transaction
}
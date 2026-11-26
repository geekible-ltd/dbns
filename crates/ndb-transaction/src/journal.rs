//! The journaling hook connecting the buffer pool to the WAL (ADR-006).
//!
//! The buffer pool calls the installed [`PageHook`] every time a
//! write-latched page is released with modified bytes. The hook records a
//! `PageUpdate` WAL record under the *currently active* transaction — safe
//! to derive globally because the transaction manager serializes writers
//! (ADR-007). Records written with no active transaction go to transaction
//! 0 (system), which recovery always treats as committed.
//!

use ndb_buffer::PageHook;
use ndb_common::error::Error;
use ndb_common::ids::{PageId, TransactionId};
use ndb_wal::{Wal, WalRecord};
use std::sync::{Arc, Mutex};

/// Transaction id used for journal records written outside a transaction
/// (e.g. checkpoint-time flushes). Recovery always redoes these.
pub const SYSTEM_TXN: TransactionId = TransactionId::new(0);

/// Shared journal state; installed on the pool as the page hook.
pub struct Journal {
    wal: Arc<Wal>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Currently active write transaction, if any.
    active: Option<TransactionId>,
    /// Latched WAL error — checked after every operation.
    err: Option<Error>,
}

impl Journal {
    pub fn new(wal: Arc<Wal>) -> Arc<Self> {
        Arc::new(Journal {
            wal,
            state: Mutex::new(State::default()),
        })
    }

    /// The page-hook closure to install on the pool.
    pub fn hook(self: &Arc<Self>) -> Arc<PageHook> {
        let journal = Arc::clone(self);
        Arc::new(move |page_id, before, after| journal.record(page_id, before, after))
    }

    /// Invoked by the pool on write-guard drop; returns the record LSN.
    fn record(&self, page_id: PageId, before: &[u8], after: &[u8]) -> u64 {
        let mut st = self.state.lock().unwrap();
        if st.err.is_some() {
            return 0; // already failed; ops will surface the error
        }
        let txn = st.active.unwrap_or(SYSTEM_TXN);
        match self.wal.append(
            txn,
            WalRecord::PageUpdate {
                page_id,
                before: before.to_vec(),
                after: after.to_vec(),
            },
        ) {
            Ok(lsn) => lsn.get(),
            Err(e) => {
                st.err = Some(e);
                0
            }
        }
    }

    /// Marks `txn` as the active write transaction (or clears it).
    pub fn set_active(&self, txn: Option<TransactionId>) {
        self.state.lock().unwrap().active = txn;
    }

    /// Surfaces a stashed WAL error, if any, and clears it.
    pub fn check_err(&self) -> ndb_common::Result<()> {
        match self.state.lock().unwrap().err.take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Test hook (spec §22.3 fault injection): latches `e` so the next
    /// `check_err` fails — exercises the torn-commit compensation path
    /// without needing a fault-injecting filesystem.
    #[doc(hidden)]
    pub fn inject_error(&self, e: Error) {
        self.state.lock().unwrap().err = Some(e);
    }
}

// ledger/src/account.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A **descriptive** record of what a ledger owner id represents.
///
/// A balance in this ledger is an aggregate over `ledger_value_objects`
/// grouped by `owner` — there is no row anywhere that *is* an account. That
/// works fine as long as every owner id is a row in some other table you
/// can look up. It stops working the moment you need to answer:
///
/// - "list every internal account and its balance" — impossible before
///   this type, because every read on the ledger is keyed **by** owner;
///   there was no way to enumerate owners at all.
/// - "whose money is this?" for an owner id found in a transaction log.
/// - "what do we owe partner X?" when partner accounts are created
///   dynamically rather than being a fixed, hard-coded set.
///
/// # Registration is optional
///
/// Nothing enforces that an owner is registered, and there is deliberately
/// no foreign key from `ledger_value_objects` to `ledger_accounts`. Every
/// balance that existed before this table keeps working untouched; an
/// unregistered owner simply has no [`Account`] to return. This type
/// describes accounts, it does not gate them.
///
/// # `key` is the durable identity
///
/// `owner` is whatever uuid the application settles against — usually
/// another table's primary key. If that row is ever lost, the uuid alone
/// says nothing and the balance is unattributable. `key` is the guard
/// against that: a stable, human-meaningful string (`"partner:fastlink"`,
/// `"mealgro-platform"`) that identifies the account independently of any
/// other table. Choose it from something that does not change.
///
/// # Never deleted
///
/// There is no delete. An account that is finished is
/// [`archived`](Account::is_archived) — a row that has ever held money
/// stays readable forever, because the whole point is to still be able to
/// explain a balance long after whatever created it is gone.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Account {
    /// The ledger owner id this describes — what you pass to
    /// `get_balance`, `reserve`, `settle`, and friends.
    pub owner: Uuid,
    /// Stable, unique, human-meaningful identity. Survives the loss of
    /// whatever row `owner` came from.
    pub key: String,
    /// Display name.
    pub label: String,
    /// Free-form application category (`"user"`, `"store"`, `"partner"`,
    /// `"system"`, …). The ledger never interprets it; it exists so
    /// [`list_accounts`](crate::LedgerAdapter::list_accounts) can filter.
    pub kind: String,
    /// Arbitrary application data. Kept out of the ledger's own logic
    /// entirely — this is a label, not state the ledger reasons about.
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Set when the account is retired. Archived accounts still resolve
    /// through every lookup; they are excluded from `list_accounts` unless
    /// asked for.
    pub archived_at: Option<DateTime<Utc>>,
}

impl Account {
    pub fn new(owner: Uuid, key: impl Into<String>, label: impl Into<String>, kind: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            owner,
            key: key.into(),
            label: label.into(),
            kind: kind.into(),
            metadata: serde_json::Value::Object(Default::default()),
            created_at: now,
            updated_at: now,
            archived_at: None,
        }
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn is_archived(&self) -> bool {
        self.archived_at.is_some()
    }
}

/// Filter for [`LedgerAdapter::list_accounts`](crate::LedgerAdapter::list_accounts).
///
/// Defaults to "every live account, newest first, 100 at a time".
#[derive(Debug, Clone, Default)]
pub struct AccountQuery {
    /// Restrict to one application category.
    pub kind: Option<String>,
    /// Include archived accounts. Off by default — an archived account is
    /// normally noise in a listing, and forgetting to exclude them is the
    /// easier mistake to make.
    pub include_archived: bool,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

impl AccountQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn of_kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }

    pub fn including_archived(mut self) -> Self {
        self.include_archived = true;
        self
    }

    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

    pub(crate) fn effective_limit(&self) -> i64 {
        self.limit.unwrap_or(100).min(1_000) as i64
    }

    pub(crate) fn effective_offset(&self) -> i64 {
        self.offset.unwrap_or(0) as i64
    }
}

/// An [`Account`] paired with its balance in one asset — the row an
/// "internal accounts" dashboard renders.
#[derive(Debug, Clone)]
pub struct AccountBalance {
    pub account: Account,
    pub balance: crate::Balance,
}

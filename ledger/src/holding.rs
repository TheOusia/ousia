// ledger/src/holding.rs
use crate::{Asset, Balance};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Holding {
    pub asset: Asset,
    pub balance: Balance,
}

impl Holding {
    pub fn new(asset: Asset, balance: Balance) -> Self {
        Self { asset, balance }
    }

    /// Display quantity of this holding (e.g. 2.5 BTC, not 250_000_000 satoshis).
    pub fn quantity(&self) -> f64 {
        self.asset.to_display(self.balance.total)
    }

    /// Value in a target currency: quantity * rate.
    /// Rate is how much 1 unit of this asset is worth in the target currency.
    pub fn value(&self, rate: f64) -> f64 {
        self.quantity() * rate
    }
}

/// An owned collection of holdings that can be queried and sorted as a unit.
///
/// Construct from the result of `LedgerContext::holdings`:
/// ```ignore
/// let portfolio: Portfolio = ctx.holdings(owner).await?.into();
/// ```
#[derive(Debug, Clone)]
pub struct Portfolio {
    holdings: Vec<Holding>,
}

impl Portfolio {
    pub fn new(holdings: Vec<Holding>) -> Self {
        Self { holdings }
    }

    pub fn holdings(&self) -> &[Holding] {
        &self.holdings
    }

    pub fn len(&self) -> usize {
        self.holdings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.holdings.is_empty()
    }

    /// Find a holding by asset code.
    pub fn get(&self, asset_code: &str) -> Option<&Holding> {
        self.holdings.iter().find(|h| h.asset.code == asset_code)
    }

    /// Total portfolio value in a target currency.
    ///
    /// `rates` maps asset code → exchange rate into the target currency.
    /// Assets absent from `rates` contribute 0.
    ///
    /// Example — total in USD:
    /// ```ignore
    /// let rates = HashMap::from([("BTC", 60_000.0), ("ETH", 3_000.0), ("USDC", 1.0)]);
    /// let total_usd = portfolio.value(&rates);
    /// ```
    pub fn value(&self, rates: &HashMap<&str, f64>) -> f64 {
        self.holdings
            .iter()
            .map(|h| {
                let rate = rates.get(h.asset.code.as_str()).copied().unwrap_or(0.0);
                h.value(rate)
            })
            .sum()
    }

    /// Sort holdings by quantity, largest first.
    pub fn sort_by_largest(&mut self) {
        self.holdings.sort_by(|a, b| {
            b.quantity()
                .partial_cmp(&a.quantity())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    /// Sort holdings by quantity, smallest first.
    pub fn sort_by_smallest(&mut self) {
        self.holdings.sort_by(|a, b| {
            a.quantity()
                .partial_cmp(&b.quantity())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    /// Sort holdings by value in a target currency, largest first.
    pub fn sort_by_value_desc(&mut self, rates: &HashMap<&str, f64>) {
        self.holdings.sort_by(|a, b| {
            let a_rate = rates.get(a.asset.code.as_str()).copied().unwrap_or(0.0);
            let b_rate = rates.get(b.asset.code.as_str()).copied().unwrap_or(0.0);
            b.value(b_rate)
                .partial_cmp(&a.value(a_rate))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    /// Sort holdings by value in a target currency, smallest first.
    pub fn sort_by_value_asc(&mut self, rates: &HashMap<&str, f64>) {
        self.holdings.sort_by(|a, b| {
            let a_rate = rates.get(a.asset.code.as_str()).copied().unwrap_or(0.0);
            let b_rate = rates.get(b.asset.code.as_str()).copied().unwrap_or(0.0);
            a.value(a_rate)
                .partial_cmp(&b.value(b_rate))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
}

impl From<Vec<Holding>> for Portfolio {
    fn from(holdings: Vec<Holding>) -> Self {
        Self::new(holdings)
    }
}

impl IntoIterator for Portfolio {
    type Item = Holding;
    type IntoIter = std::vec::IntoIter<Holding>;

    fn into_iter(self) -> Self::IntoIter {
        self.holdings.into_iter()
    }
}

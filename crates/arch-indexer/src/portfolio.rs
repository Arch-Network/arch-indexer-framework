//! The shapes a cross-program host (arch-prime-analytics' portfolio and
//! trades engines) composes from program crates: a [`PortfolioLeg`] each
//! crate values its positions with, and the [`Conversion`]s each crate reads
//! out of a transaction.

use crate::prices::Prices;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use std::collections::HashSet;

/// What one leg holds at one instant, in USD: what it owns and what it owes.
/// A wallet's net worth adds `assets_usd` and subtracts `liabilities_usd`
/// over every leg.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct LegValue {
    pub assets_usd: f64,
    pub liabilities_usd: f64,
}

impl LegValue {
    pub fn net_usd(&self) -> f64 {
        self.assets_usd - self.liabilities_usd
    }
}

/// One program's positions for one address, loaded once and valued at every
/// bucket of a portfolio series (e.g. CLAMM's LP positions, Arch Lend's
/// balances).
pub trait PortfolioLeg {
    /// The leg's first activity, so the series starts no later. `None` when
    /// the address has none.
    fn start(&self) -> Option<DateTime<Utc>>;

    /// Every mint [`Self::value_at`] prices, so the host loads their candles.
    fn mints(&self) -> HashSet<String>;

    /// The leg's value at `at`, an exclusive bucket-end instant (see
    /// [`crate::prices::obs_at`]). A mint the spine does not price yet
    /// contributes 0.
    fn value_at(&self, prices: &Prices, at: DateTime<Utc>) -> LegValue;
}

/// How a conversion changed what an account holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConversionKind {
    /// A market trade at a price (a CLAMM swap): analytics' `venue == "clamm"`.
    Swap,
    /// A conversion at the program's own rate into a claim on the same asset
    /// (a vault share mint, or a redeem filled in the same transaction):
    /// analytics' `venue == "vault"`.
    Wrap,
}

/// One conversion in a transaction: `amount_in` of `mint_in` became
/// `amount_out` of `mint_out` through `pool` (the pool or the vault), the
/// fee in `mint_in` atoms. Atoms throughout; a field is `None` where the
/// program's record does not carry it.
#[derive(Debug, Clone, PartialEq)]
pub struct Conversion {
    pub txid: String,
    pub ix_path: String,
    pub kind: ConversionKind,
    pub pool: String,
    pub mint_in: Option<String>,
    pub mint_out: Option<String>,
    pub amount_in: Option<BigDecimal>,
    pub amount_out: Option<BigDecimal>,
    pub fee_atoms: Option<BigDecimal>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prices::cum_at;
    use std::collections::HashMap;

    fn ts(h: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(h * 3600, 0).unwrap()
    }

    /// A one-mint balance with debt, as a lending leg would report it.
    struct Balance {
        mint: String,
        supplied: Vec<(DateTime<Utc>, f64)>,
        borrowed: Vec<(DateTime<Utc>, f64)>,
    }

    impl PortfolioLeg for Balance {
        fn start(&self) -> Option<DateTime<Utc>> {
            self.supplied.first().map(|(t, _)| *t)
        }
        fn mints(&self) -> HashSet<String> {
            HashSet::from([self.mint.clone()])
        }
        fn value_at(&self, prices: &Prices, at: DateTime<Utc>) -> LegValue {
            let p = prices.at(&self.mint, at).unwrap_or(0.0);
            LegValue {
                assets_usd: cum_at(&self.supplied, at) * p,
                liabilities_usd: cum_at(&self.borrowed, at) * p,
            }
        }
    }

    #[test]
    fn a_host_composes_legs_behind_the_trait() {
        let legs: Vec<Box<dyn PortfolioLeg>> = vec![
            Box::new(Balance {
                mint: "btc".into(),
                supplied: vec![(ts(2), 2.0)],
                borrowed: vec![(ts(3), 0.5)],
            }),
            Box::new(Balance {
                mint: "usd".into(),
                supplied: vec![(ts(1), 100.0)],
                borrowed: vec![],
            }),
        ];
        let prices = Prices(HashMap::from([
            ("btc".to_string(), vec![(ts(0), 10.0)]),
            ("usd".to_string(), vec![(ts(0), 1.0)]),
        ]));
        let start = legs.iter().filter_map(|l| l.start()).min();
        assert_eq!(start, Some(ts(1)));
        let mints: HashSet<String> = legs.iter().flat_map(|l| l.mints()).collect();
        assert_eq!(mints.len(), 2);
        let net: f64 = legs
            .iter()
            .map(|l| l.value_at(&prices, ts(4)).net_usd())
            .sum();
        // 2 BTC × 10 − 0.5 BTC × 10 + 100 USD.
        assert_eq!(net, 115.0);
        assert_eq!(
            legs[0].value_at(&prices, ts(3)),
            LegValue {
                assets_usd: 20.0,
                liabilities_usd: 0.0
            }
        );
    }
}

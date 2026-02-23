use chrono::{Datelike, NaiveDateTime};
use csv::{ReaderBuilder, WriterBuilder};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone)]
struct LedgerRow {
    txid: String,
    refid: String,
    time: String,
    #[serde(rename = "type")]
    row_type: String,
    #[serde(default)]
    subtype: String,
    asset: String,
    amount: String,
    fee: String,
}

#[derive(Debug, Clone)]
struct LedgerEntry {
    txid: String,
    refid: String,
    time: NaiveDateTime,
    row_type: String,
    subtype: String,
    asset: String,
    amount: Decimal,
    fee: Decimal,
    net_delta: Decimal,
}

#[derive(Debug, Clone)]
struct TradeGroup {
    refid: String,
    time: NaiveDateTime,
    txid: String,
    entries: Vec<LedgerEntry>,
}

#[derive(Debug, Clone)]
enum Event {
    Trade(TradeGroup),
    Entry(LedgerEntry),
}

#[derive(Debug, Default, Clone)]
struct Pool {
    units: Decimal,
    acb_cad: Decimal,
}

impl Pool {
    fn avg_cost_cad_per_unit(&self) -> Decimal {
        if self.units.is_zero() {
            dec!(0)
        } else {
            self.acb_cad / self.units
        }
    }
}

#[derive(Debug, Default)]
struct Totals {
    proceeds_cad: Decimal,
    acb_disposed_cad: Decimal,
    capital_gain_cad: Decimal,
    reward_income_cad: Decimal,
    warning_count: usize,
}

#[derive(Debug, Default)]
struct PriceState {
    usd_cad_last: Option<Decimal>,
    asset_price_usd: HashMap<String, Decimal>,
    asset_price_cad: HashMap<String, Decimal>,
}

#[derive(Debug, Serialize)]
struct ReportRow {
    time: String,
    refid: String,
    txid: String,
    event_type: String,
    asset: String,
    units_in: String,
    units_out: String,
    proceeds_cad: String,
    acb_disposed_cad: String,
    gain_cad: String,
    income_cad: String,
    acb_added_cad: String,
    pool_units_after: String,
    pool_acb_cad_after: String,
    notes: String,
}

#[derive(Debug)]
struct Args {
    input: String,
    tax_year: i32,
    output: String,
    fallback_usd_cad_fx: Decimal,
}

fn parse_decimal(s: &str) -> Result<Decimal, Box<dyn Error>> {
    Ok(Decimal::from_str(s.trim())?)
}

fn parse_time(s: &str) -> Result<NaiveDateTime, Box<dyn Error>> {
    let s = s.trim();
    if let Ok(t) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(t);
    }
    if let Ok(t) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Ok(t);
    }
    Err(format!("unsupported timestamp format: {}", s).into())
}

fn q2(x: Decimal) -> Decimal {
    x.round_dp_with_strategy(2, RoundingStrategy::MidpointAwayFromZero)
}

fn q8(x: Decimal) -> Decimal {
    x.round_dp_with_strategy(8, RoundingStrategy::MidpointAwayFromZero)
}

fn parse_args() -> Result<Args, Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let input = args
        .next()
        .unwrap_or_else(|| "kraken_2024_2025_ledgers.csv".to_string());
    let tax_year: i32 = args.next().unwrap_or_else(|| "2025".to_string()).parse()?;
    let output = args
        .next()
        .unwrap_or_else(|| format!("kraken_tax_report_{}.csv", tax_year));
    let fallback_usd_cad_fx =
        Decimal::from_str(&args.next().unwrap_or_else(|| "1.3978".to_string()))?;

    Ok(Args {
        input,
        tax_year,
        output,
        fallback_usd_cad_fx,
    })
}

fn load_entries(path: &str) -> Result<Vec<LedgerEntry>, Box<dyn Error>> {
    let f = File::open(path)?;
    let mut rdr = ReaderBuilder::new().flexible(true).from_reader(f);
    let mut out = Vec::new();

    for row in rdr.deserialize::<LedgerRow>() {
        let row = row?;
        let amount = parse_decimal(&row.amount)?;
        let fee = parse_decimal(&row.fee)?;
        out.push(LedgerEntry {
            txid: row.txid,
            refid: row.refid,
            time: parse_time(&row.time)?,
            row_type: row.row_type.trim().to_lowercase(),
            subtype: row.subtype.trim().to_lowercase(),
            asset: row.asset.trim().to_uppercase(),
            amount,
            fee,
            net_delta: amount - fee,
        });
    }

    out.sort_by(|a, b| {
        a.time
            .cmp(&b.time)
            .then(a.refid.cmp(&b.refid))
            .then(a.txid.cmp(&b.txid))
            .then(a.asset.cmp(&b.asset))
    });

    Ok(out)
}

fn build_trade_groups(
    entries: &[LedgerEntry],
    tax_year: i32,
) -> Result<HashMap<String, TradeGroup>, Box<dyn Error>> {
    let mut tmp: HashMap<String, Vec<LedgerEntry>> = HashMap::new();
    for e in entries {
        if e.time.year() > tax_year {
            continue;
        }
        if e.row_type == "trade" && e.subtype == "tradespot" {
            tmp.entry(e.refid.clone()).or_default().push(e.clone());
        }
    }

    let mut groups = HashMap::new();
    for (refid, mut rows) in tmp {
        rows.sort_by(|a, b| a.txid.cmp(&b.txid).then(a.asset.cmp(&b.asset)));
        if rows.len() != 2 {
            return Err(
                format!("trade refid {} expected 2 rows, got {}", refid, rows.len()).into(),
            );
        }
        let time = rows[0].time;
        if rows[1].time != time {
            return Err(format!("trade refid {} has mismatched times", refid).into());
        }
        groups.insert(
            refid.clone(),
            TradeGroup {
                refid,
                time,
                txid: rows[0].txid.clone(),
                entries: rows,
            },
        );
    }

    Ok(groups)
}

fn build_events(
    entries: &[LedgerEntry],
    trade_groups: &HashMap<String, TradeGroup>,
    tax_year: i32,
) -> Vec<Event> {
    let mut events = Vec::new();
    let mut emitted_trade = HashSet::new();

    for e in entries {
        if e.time.year() > tax_year {
            continue;
        }
        if e.row_type == "trade" && e.subtype == "tradespot" {
            if emitted_trade.insert(e.refid.clone()) {
                if let Some(g) = trade_groups.get(&e.refid) {
                    events.push(Event::Trade(g.clone()));
                }
            }
        } else {
            events.push(Event::Entry(e.clone()));
        }
    }

    events.sort_by(|a, b| {
        let (ta, ka, sa) = event_sort_keys(a);
        let (tb, kb, sb) = event_sort_keys(b);
        ta.cmp(&tb).then(ka.cmp(&kb)).then(sa.cmp(&sb))
    });

    events
}

fn event_sort_keys(e: &Event) -> (NaiveDateTime, i32, String) {
    match e {
        Event::Trade(t) => (t.time, 0, format!("{}:{}", t.refid, t.txid)),
        Event::Entry(x) => (x.time, 1, format!("{}:{}:{}", x.refid, x.txid, x.asset)),
    }
}

fn split_trade_legs(g: &TradeGroup) -> Result<(LedgerEntry, LedgerEntry), Box<dyn Error>> {
    let a = &g.entries[0];
    let b = &g.entries[1];
    let (out, inn) = if a.net_delta < dec!(0) && b.net_delta > dec!(0) {
        (a.clone(), b.clone())
    } else if b.net_delta < dec!(0) && a.net_delta > dec!(0) {
        (b.clone(), a.clone())
    } else {
        return Err(format!(
            "trade refid {} not reducible to one outflow and one inflow",
            g.refid
        )
        .into());
    };

    if out.net_delta.is_zero() || inn.net_delta.is_zero() {
        return Err(format!("trade refid {} has zero net leg", g.refid).into());
    }

    Ok((out, inn))
}

fn usd_cad_rate(state: &PriceState, fallback_fx: Decimal) -> Decimal {
    state.usd_cad_last.unwrap_or(fallback_fx)
}

fn asset_value_cad(
    asset: &str,
    units: Decimal,
    state: &PriceState,
    fallback_fx: Decimal,
    ctx: &str,
) -> Result<Decimal, Box<dyn Error>> {
    if units.is_zero() {
        return Ok(dec!(0));
    }

    if asset == "CAD" {
        return Ok(units);
    }
    if asset == "USD" {
        return Ok(units * usd_cad_rate(state, fallback_fx));
    }
    if let Some(p) = state.asset_price_cad.get(asset) {
        return Ok(units * *p);
    }
    if let Some(p_usd) = state.asset_price_usd.get(asset) {
        return Ok(units * *p_usd * usd_cad_rate(state, fallback_fx));
    }

    Err(format!("missing valuation price for {} in {}", asset, ctx).into())
}

fn remove_units_at_acb(
    pool: &mut Pool,
    units: Decimal,
    ctx: &str,
) -> Result<Decimal, Box<dyn Error>> {
    if units < dec!(0) {
        return Err(format!("negative removal units in {}", ctx).into());
    }
    if units > pool.units {
        return Err(format!(
            "insufficient units in {}: remove={}, pool={}",
            ctx, units, pool.units
        )
        .into());
    }

    let avg = pool.avg_cost_cad_per_unit();
    let acb = avg * units;
    pool.units -= units;
    pool.acb_cad -= acb;
    if pool.units.is_zero() {
        pool.acb_cad = dec!(0);
    }
    Ok(acb)
}

fn update_prices_from_trade(
    out: &LedgerEntry,
    inn: &LedgerEntry,
    state: &mut PriceState,
    fallback_fx: Decimal,
) {
    let out_units = -out.net_delta;
    let in_units = inn.net_delta;
    if out_units <= dec!(0) || in_units <= dec!(0) {
        return;
    }

    if (out.asset == "USD" && inn.asset == "CAD") || (out.asset == "CAD" && inn.asset == "USD") {
        let usd = if out.asset == "USD" {
            out_units
        } else {
            in_units
        };
        let cad = if out.asset == "CAD" {
            out_units
        } else {
            in_units
        };
        if usd > dec!(0) {
            let fx = cad / usd;
            state.usd_cad_last = Some(fx);
            state.asset_price_cad.insert("USD".to_string(), fx);
        }
    }

    if out.asset == "USD" && inn.asset != "CAD" {
        state
            .asset_price_usd
            .insert(inn.asset.clone(), out_units / in_units);
    }
    if inn.asset == "USD" && out.asset != "CAD" {
        state
            .asset_price_usd
            .insert(out.asset.clone(), in_units / out_units);
    }

    if out.asset == "CAD" && inn.asset != "USD" {
        state
            .asset_price_cad
            .insert(inn.asset.clone(), out_units / in_units);
    }
    if inn.asset == "CAD" && out.asset != "USD" {
        state
            .asset_price_cad
            .insert(out.asset.clone(), in_units / out_units);
    }

    let fx = usd_cad_rate(state, fallback_fx);
    for (asset, p_usd) in state.asset_price_usd.clone() {
        state.asset_price_cad.insert(asset, p_usd * fx);
    }
}

fn make_row(
    time: NaiveDateTime,
    refid: &str,
    txid: &str,
    event_type: &str,
    asset: &str,
) -> ReportRow {
    ReportRow {
        time: format!("{}+00:00", time.format("%Y-%m-%dT%H:%M:%S%.f")),
        refid: refid.to_string(),
        txid: txid.to_string(),
        event_type: event_type.to_string(),
        asset: asset.to_string(),
        units_in: String::new(),
        units_out: String::new(),
        proceeds_cad: String::new(),
        acb_disposed_cad: String::new(),
        gain_cad: String::new(),
        income_cad: String::new(),
        acb_added_cad: String::new(),
        pool_units_after: String::new(),
        pool_acb_cad_after: String::new(),
        notes: String::new(),
    }
}

fn process(
    entries: Vec<LedgerEntry>,
    tax_year: i32,
    fallback_fx: Decimal,
) -> Result<(Vec<ReportRow>, Totals, HashMap<String, Pool>), Box<dyn Error>> {
    let trade_groups = build_trade_groups(&entries, tax_year)?;
    let events = build_events(&entries, &trade_groups, tax_year);

    let mut pools: HashMap<String, Pool> = HashMap::new();
    let mut state = PriceState::default();
    let mut report = Vec::new();
    let mut totals = Totals::default();

    for ev in events {
        match ev {
            Event::Trade(g) => {
                let (out, inn) = split_trade_legs(&g)?;
                let out_units = -out.net_delta;
                let in_units = inn.net_delta;

                let out_cad = if out.asset == "CAD" {
                    out_units
                } else if out.asset == "USD" {
                    out_units * usd_cad_rate(&state, fallback_fx)
                } else if inn.asset == "CAD" {
                    in_units
                } else if inn.asset == "USD" {
                    in_units * usd_cad_rate(&state, fallback_fx)
                } else {
                    asset_value_cad(
                        &out.asset,
                        out_units,
                        &state,
                        fallback_fx,
                        &format!("trade {} out leg", g.refid),
                    )?
                };

                let in_cad = if inn.asset == "CAD" {
                    in_units
                } else if inn.asset == "USD" {
                    in_units * usd_cad_rate(&state, fallback_fx)
                } else if out.asset == "CAD" {
                    out_units
                } else if out.asset == "USD" {
                    out_units * usd_cad_rate(&state, fallback_fx)
                } else {
                    asset_value_cad(
                        &inn.asset,
                        in_units,
                        &state,
                        fallback_fx,
                        &format!("trade {} in leg", g.refid),
                    )?
                };

                if out.asset != "CAD" {
                    let pool = pools.entry(out.asset.clone()).or_default();
                    let acb_disposed = remove_units_at_acb(
                        pool,
                        out_units,
                        &format!("trade disposition {} {}", g.refid, out.asset),
                    )?;
                    let gain = in_cad - acb_disposed;

                    if g.time.year() == tax_year {
                        let mut rr =
                            make_row(g.time, &g.refid, &g.txid, "trade_disposition", &out.asset);
                        rr.units_out = q8(out_units).to_string();
                        rr.proceeds_cad = q2(in_cad).to_string();
                        rr.acb_disposed_cad = q2(acb_disposed).to_string();
                        rr.gain_cad = q2(gain).to_string();
                        rr.pool_units_after = q8(pool.units).to_string();
                        rr.pool_acb_cad_after = q2(pool.acb_cad).to_string();
                        report.push(rr);

                        totals.proceeds_cad += in_cad;
                        totals.acb_disposed_cad += acb_disposed;
                        totals.capital_gain_cad += gain;
                    }
                }

                if inn.asset != "CAD" {
                    let pool = pools.entry(inn.asset.clone()).or_default();
                    pool.units += in_units;
                    pool.acb_cad += out_cad;

                    if g.time.year() == tax_year {
                        let mut rr =
                            make_row(g.time, &g.refid, &g.txid, "trade_acquisition", &inn.asset);
                        rr.units_in = q8(in_units).to_string();
                        rr.acb_added_cad = q2(out_cad).to_string();
                        rr.pool_units_after = q8(pool.units).to_string();
                        rr.pool_acb_cad_after = q2(pool.acb_cad).to_string();
                        report.push(rr);
                    }
                }

                update_prices_from_trade(&out, &inn, &mut state, fallback_fx);
            }
            Event::Entry(e) => match (e.row_type.as_str(), e.subtype.as_str()) {
                ("earn", "reward") => {
                    if e.net_delta <= dec!(0) {
                        return Err(format!(
                            "earn reward must be positive net for refid {}",
                            e.refid
                        )
                        .into());
                    }
                    let income_cad = asset_value_cad(
                        &e.asset,
                        e.net_delta,
                        &state,
                        fallback_fx,
                        &format!("earn reward {}", e.refid),
                    )?;

                    if e.asset != "CAD" {
                        let pool = pools.entry(e.asset.clone()).or_default();
                        pool.units += e.net_delta;
                        pool.acb_cad += income_cad;

                        if e.time.year() == tax_year {
                            let mut rr =
                                make_row(e.time, &e.refid, &e.txid, "earn_reward_income", &e.asset);
                            rr.units_in = q8(e.net_delta).to_string();
                            rr.income_cad = q2(income_cad).to_string();
                            rr.acb_added_cad = q2(income_cad).to_string();
                            rr.pool_units_after = q8(pool.units).to_string();
                            rr.pool_acb_cad_after = q2(pool.acb_cad).to_string();
                            report.push(rr);
                            totals.reward_income_cad += income_cad;
                        }
                    }
                }
                ("earn", "autoallocation") | ("earn", "allocation") | ("earn", "deallocation") => {
                    // Internal wallet movements; pooled holdings are unchanged.
                }
                ("deposit", "") => {
                    if e.net_delta <= dec!(0) {
                        return Err(format!(
                            "deposit with non-positive net delta at refid {}",
                            e.refid
                        )
                        .into());
                    }
                    if e.asset != "CAD" {
                        let pool = pools.entry(e.asset.clone()).or_default();
                        pool.units += e.net_delta;

                        if e.time.year() == tax_year {
                            let mut rr = make_row(
                                e.time,
                                &e.refid,
                                &e.txid,
                                "warning_unpriced_transfer_in",
                                &e.asset,
                            );
                            rr.units_in = q8(e.net_delta).to_string();
                            rr.pool_units_after = q8(pool.units).to_string();
                            rr.pool_acb_cad_after = q2(pool.acb_cad).to_string();
                            rr.notes = "Deposit treated as transfer-in with unknown ACB; assumed 0 CAD basis".to_string();
                            report.push(rr);
                            totals.warning_count += 1;
                        }
                    }
                }
                ("withdrawal", "") => {
                    if e.amount >= dec!(0) {
                        return Err(format!(
                            "withdrawal amount must be negative at refid {}",
                            e.refid
                        )
                        .into());
                    }
                    let principal_units = -e.amount;
                    let fee_units = e.fee;

                    if e.asset != "CAD" {
                        let pool = pools.entry(e.asset.clone()).or_default();

                        let _principal_acb = remove_units_at_acb(
                            pool,
                            principal_units,
                            &format!("withdrawal principal {} {}", e.refid, e.asset),
                        )?;

                        if fee_units > dec!(0) {
                            let acb_fee = remove_units_at_acb(
                                pool,
                                fee_units,
                                &format!("withdrawal fee {} {}", e.refid, e.asset),
                            )?;
                            let gain = -acb_fee;

                            if e.time.year() == tax_year {
                                let mut rr = make_row(
                                    e.time,
                                    &e.refid,
                                    &e.txid,
                                    "withdrawal_fee_disposition",
                                    &e.asset,
                                );
                                rr.units_out = q8(fee_units).to_string();
                                rr.proceeds_cad = "0".to_string();
                                rr.acb_disposed_cad = q2(acb_fee).to_string();
                                rr.gain_cad = q2(gain).to_string();
                                rr.pool_units_after = q8(pool.units).to_string();
                                rr.pool_acb_cad_after = q2(pool.acb_cad).to_string();
                                report.push(rr);

                                totals.proceeds_cad += dec!(0);
                                totals.acb_disposed_cad += acb_fee;
                                totals.capital_gain_cad += gain;
                            }
                        }
                    }
                }
                _ => {
                    // Unknown/non-tax-relevant ledger types are ignored by default.
                }
            },
        }
    }

    Ok((report, totals, pools))
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;

    let input_path = PathBuf::from(&args.input);
    if !input_path.exists() {
        return Err(format!("CSV not found: {:?}", input_path).into());
    }

    let entries = load_entries(&args.input)?;
    let (report, totals, pools) = process(entries, args.tax_year, args.fallback_usd_cad_fx)?;

    let out_file = File::create(&args.output)?;
    let mut wtr = WriterBuilder::new().from_writer(out_file);
    for row in report {
        wtr.serialize(row)?;
    }
    wtr.flush()?;

    println!("\n=== CANADIAN CRYPTO TAX SUMMARY (LEDGER / ACB) ===");
    println!("Tax year: {}", args.tax_year);
    println!("Fallback USD/CAD FX: {}", args.fallback_usd_cad_fx);
    println!("Total proceeds (CAD): {}", q2(totals.proceeds_cad));
    println!("Total ACB disposed (CAD): {}", q2(totals.acb_disposed_cad));
    println!(
        "Net capital gain/loss (CAD): {}",
        q2(totals.capital_gain_cad)
    );
    println!(
        "Total reward income (CAD): {}",
        q2(totals.reward_income_cad)
    );
    println!(
        "Warnings (transfer-in assumed 0 ACB): {}",
        totals.warning_count
    );

    println!("\n=== ENDING POOLS (units + ACB) ===");
    let mut assets: Vec<_> = pools.keys().cloned().collect();
    assets.sort();
    for asset in assets {
        if asset == "CAD" {
            continue;
        }
        if let Some(p) = pools.get(&asset) {
            println!(
                "{}: units={}, ACB(CAD)={}, avg_cost(CAD/unit)={}",
                asset,
                q8(p.units),
                q2(p.acb_cad),
                q2(p.avg_cost_cad_per_unit())
            );
        }
    }

    println!("\nWrote tax report: {}", args.output);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(
        time: &str,
        txid: &str,
        refid: &str,
        row_type: &str,
        subtype: &str,
        asset: &str,
        amount: &str,
        fee: &str,
    ) -> LedgerEntry {
        let amount_d = Decimal::from_str(amount).unwrap();
        let fee_d = Decimal::from_str(fee).unwrap();
        LedgerEntry {
            txid: txid.to_string(),
            refid: refid.to_string(),
            time: parse_time(time).unwrap(),
            row_type: row_type.to_string(),
            subtype: subtype.to_string(),
            asset: asset.to_string(),
            amount: amount_d,
            fee: fee_d,
            net_delta: amount_d - fee_d,
        }
    }

    #[test]
    fn parses_timestamp_with_fraction() {
        let t = parse_time("2025-01-04 00:05:16.8462").unwrap();
        assert_eq!(t.year(), 2025);
        assert_eq!(t.month(), 1);
    }

    #[test]
    fn net_delta_calculates_with_fee() {
        let e = entry(
            "2025-01-01 00:00:00",
            "T1",
            "R1",
            "withdrawal",
            "",
            "ETH",
            "-1.0",
            "0.01",
        );
        assert_eq!(e.net_delta, dec!(-1.01));
    }

    #[test]
    fn trade_group_requires_two_rows() {
        let entries = vec![entry(
            "2025-01-01 00:00:00",
            "T1",
            "R1",
            "trade",
            "tradespot",
            "USD",
            "-100",
            "1",
        )];
        let err = build_trade_groups(&entries, 2025).unwrap_err().to_string();
        assert!(err.contains("expected 2 rows"));
    }

    #[test]
    fn usd_cad_fallback_is_used() {
        let state = PriceState::default();
        assert_eq!(usd_cad_rate(&state, dec!(1.4)), dec!(1.4));
    }

    #[test]
    fn withdrawal_fee_creates_loss() {
        let entries = vec![
            entry(
                "2024-12-01 00:00:00",
                "T1",
                "R1",
                "trade",
                "tradespot",
                "CAD",
                "-140.0",
                "0",
            ),
            entry(
                "2024-12-01 00:00:00",
                "T2",
                "R1",
                "trade",
                "tradespot",
                "SOL",
                "1.0",
                "0",
            ),
            entry(
                "2025-01-01 00:00:00",
                "T3",
                "R2",
                "withdrawal",
                "",
                "SOL",
                "-0.5",
                "0.1",
            ),
        ];

        let (rows, totals, pools) = process(entries, 2025, dec!(1.4)).unwrap();
        assert!(rows
            .iter()
            .any(|r| r.event_type == "withdrawal_fee_disposition"));
        assert!(totals.capital_gain_cad < dec!(0));
        let sol = pools.get("SOL").unwrap();
        assert_eq!(q8(sol.units), dec!(0.4));
    }

    #[test]
    fn reward_income_adds_acb() {
        let entries = vec![
            entry(
                "2024-12-01 00:00:00",
                "T1",
                "R1",
                "trade",
                "tradespot",
                "CAD",
                "-140.0",
                "0",
            ),
            entry(
                "2024-12-01 00:00:00",
                "T2",
                "R1",
                "trade",
                "tradespot",
                "SOL",
                "1.0",
                "0",
            ),
            entry(
                "2025-01-01 00:00:00",
                "T3",
                "R2",
                "earn",
                "reward",
                "SOL",
                "0.2",
                "0",
            ),
        ];

        let (_rows, totals, pools) = process(entries, 2025, dec!(1.4)).unwrap();
        assert!(totals.reward_income_cad > dec!(0));
        let sol = pools.get("SOL").unwrap();
        assert_eq!(q8(sol.units), dec!(1.2));
        assert!(sol.acb_cad > dec!(0));
    }
}

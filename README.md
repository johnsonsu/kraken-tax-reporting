# Kraken ACB Tax Report Generator

Rust tools for generating a Canadian crypto tax report (ACB method) from Kraken exports.

## Current Status

- Primary tool: the ledger-based script in `src/main.rs`.

## What It Does

The script reads a Kraken **ledger CSV** (including trades, rewards, deposits, withdrawals), reconstructs pooled ACB by asset, and generates a **tax-year report**.

Implemented behavior:

- Processes full history up to the target tax year.
- Emits report rows only for the target tax year.
- Handles:
  - `trade/tradespot` grouped by `refid`
  - `earn/reward` as taxable income + ACB addition
  - `earn/autoallocation|allocation|deallocation` as internal non-taxable movements
  - `deposit` as non-taxable transfer-in (non-CAD deposits assumed 0 ACB and warned)
  - `withdrawal` as transfer-out; withdrawal fee treated as a taxable disposition
- Uses nearest-prior implied ledger prices for valuation.

## Requirements

- Rust toolchain (stable) with Cargo.

## Input Format

Expected Kraken ledger columns include:

- `txid`
- `refid`
- `time`
- `type`
- `subtype`
- `asset`
- `amount`
- `fee`

The parser is tolerant of extra columns.

## Usage

```bash
cargo run -- <ledger.csv> [tax_year] [out.csv] [fallback_usd_cad_fx]
```

Defaults:

- `tax_year = 2025`
- `out.csv = kraken_tax_report_<tax_year>.csv`
- `fallback_usd_cad_fx = 1.3978`

Example:

```bash
cargo run -- ./kraken_2024_2025_ledgers.csv 2025 report_2025.csv 1.3978
```

## Download Prebuilt Binaries

Prebuilt binaries are published on GitHub Releases:

- [Releases](https://github.com/johnsonsu/kraken-tax-reporting/releases)

Assets are generated from version tags (for example `v0.1.1`) for:

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`
- `x86_64-pc-windows-msvc`

File naming:

- `kraken_acb-v{version}-{target}.tar.gz` (Linux/macOS)
- `kraken_acb-v{version}-{target}.zip` (Windows)
- `checksums.txt`

Linux/macOS quick run:

```bash
tar -xzf kraken_acb-v0.1.1-x86_64-unknown-linux-gnu.tar.gz
chmod +x kraken_acb
./kraken_acb ./kraken_2024_2025_ledgers.csv 2025 report_2025.csv 1.3978
```

Windows quick run (PowerShell):

```powershell
Expand-Archive .\kraken_acb-v0.1.1-x86_64-pc-windows-msvc.zip -DestinationPath .
.\kraken_acb.exe .\kraken_2024_2025_ledgers.csv 2025 report_2025.csv 1.3978
```

Verify checksums:

```bash
sha256sum -c checksums.txt
```

PowerShell checksum example:

```powershell
Get-FileHash .\kraken_acb-v0.1.1-x86_64-pc-windows-msvc.zip -Algorithm SHA256
```

## Output

### CSV report columns

- `time`
- `refid`
- `txid`
- `event_type`
- `asset`
- `units_in`
- `units_out`
- `proceeds_cad`
- `acb_disposed_cad`
- `gain_cad`
- `income_cad`
- `acb_added_cad`
- `pool_units_after`
- `pool_acb_cad_after`
- `notes`

`event_type` values:

- `trade_disposition`
- `trade_acquisition`
- `earn_reward_income`
- `withdrawal_fee_disposition`
- `warning_unpriced_transfer_in`

### Console summary

- tax year
- proceeds (CAD)
- ACB disposed (CAD)
- net capital gain/loss (CAD)
- total reward income (CAD)
- warning count
- ending pools by asset

## Valuation Rules

- USD/CAD: nearest prior implied rate from ledger `USD/CAD` trades; if unavailable, fallback to CLI FX.
- CAD assets: value at 1.0 CAD.
- USD assets: value via current USD/CAD rate.
- Other assets: nearest prior implied asset price from ledger trades (asset/CAD or asset/USD).

## Tax Assumptions in This Tool

- One pooled ACB per asset across wallets.
- CAD is base currency and not tracked as a capital property disposition here.
- Deposits are treated as transfers (not income); non-CAD deposits default to 0 ACB unless you adjust data externally.
- Rewards are treated as taxable income at receipt FMV and added to ACB.

This is a practical tax-calculation utility, not legal advice.

## Development

Run tests:

```bash
cargo test
```

Build:

```bash
cargo build
```

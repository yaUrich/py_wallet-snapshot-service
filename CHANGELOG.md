# Changelog

## [Unreleased]

### Added

- Persist one provider-backed price observation per owned asset and snapshot job
  to the shared `prices_history` table without allowing auxiliary history writes
  to fail the snapshot.
- Resolve blank manual-asset prices from crypto tickers through CoinGecko and
  ISO 4217 fiat tickers through Frankfurter exchange rates.
- Price configured ERC-20 balances by CoinGecko asset platform and contract,
  starting with mainnet WETH and WBTC while retaining symbol fallback.
- Collect native SOL and official mainnet SPL USDC/USDT balances for Solana wallets,
  with bounded RPC failover, retry support, and normalized snapshot persistence.

### Changed

- Add direct regression coverage for missing snapshot jobs, missing retry parents,
  and terminal parents without failed chains.
- Add direct regression coverage for parent-run, failed-status, owner, active-state,
  and address deduplication filters in failed-chain retry wallet loading.
- Add direct regression coverage for owner, group, active-state, and address
  deduplication filters in group-scoped snapshot wallet loading.
- Add direct regression coverage for non-fatal price-history write failures and
  successful retry after the nested transaction rolls back.
- Add direct regression coverage for missing-price and empty-wallet outcomes in
  the manual snapshot collector.
- Add direct regression coverage for enabled-chain filtering, RPC endpoint parsing,
  timeout selection, and provider metadata in snapshot chain configuration.

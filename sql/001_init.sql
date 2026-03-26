CREATE TABLE IF NOT EXISTS ingestion_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    top_n INTEGER NOT NULL,
    time_start DATE NOT NULL,
    time_end DATE NOT NULL,
    vs_currency TEXT NOT NULL,
    binance_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS coins (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    market_cap_rank INTEGER,
    current_market_cap DOUBLE PRECISION,
    current_total_volume DOUBLE PRECISION,
    current_price DOUBLE PRECISION,
    last_updated TIMESTAMPTZ,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    fetched_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS binance_klines_1h (
    symbol_pair TEXT NOT NULL,
    interval TEXT NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    coingecko_id TEXT REFERENCES coins(id) ON DELETE SET NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    quote_asset_volume DOUBLE PRECISION NOT NULL,
    trades BIGINT NOT NULL,
    taker_buy_base_asset_volume DOUBLE PRECISION NOT NULL,
    taker_buy_quote_asset_volume DOUBLE PRECISION NOT NULL,
    run_id BIGINT NOT NULL REFERENCES ingestion_runs(id) ON DELETE CASCADE,
    ingested_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol_pair, interval, open_time)
);

CREATE INDEX IF NOT EXISTS idx_binance_1h_base_open_time ON binance_klines_1h(base_asset, open_time);

CREATE TABLE IF NOT EXISTS binance_klines_4h (
    symbol_pair TEXT NOT NULL,
    interval TEXT NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    coingecko_id TEXT REFERENCES coins(id) ON DELETE SET NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    quote_asset_volume DOUBLE PRECISION NOT NULL,
    trades BIGINT NOT NULL,
    taker_buy_base_asset_volume DOUBLE PRECISION NOT NULL,
    taker_buy_quote_asset_volume DOUBLE PRECISION NOT NULL,
    run_id BIGINT NOT NULL REFERENCES ingestion_runs(id) ON DELETE CASCADE,
    ingested_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol_pair, interval, open_time)
);

CREATE INDEX IF NOT EXISTS idx_binance_4h_base_open_time ON binance_klines_4h(base_asset, open_time);

CREATE TABLE IF NOT EXISTS binance_klines_1d (
    symbol_pair TEXT NOT NULL,
    interval TEXT NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    coingecko_id TEXT REFERENCES coins(id) ON DELETE SET NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    quote_asset_volume DOUBLE PRECISION NOT NULL,
    trades BIGINT NOT NULL,
    taker_buy_base_asset_volume DOUBLE PRECISION NOT NULL,
    taker_buy_quote_asset_volume DOUBLE PRECISION NOT NULL,
    run_id BIGINT NOT NULL REFERENCES ingestion_runs(id) ON DELETE CASCADE,
    ingested_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol_pair, interval, open_time)
);

CREATE INDEX IF NOT EXISTS idx_binance_1d_base_open_time ON binance_klines_1d(base_asset, open_time);

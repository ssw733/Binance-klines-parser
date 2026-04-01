package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"ai_trading/internal/coingecko"
)

type PostgresStore struct {
	db *sql.DB
}

type BinanceRow struct {
	SymbolPair               string
	BaseAsset                string
	QuoteAsset               string
	CoinGeckoID              string
	Interval                 string
	OpenTime                 time.Time
	CloseTime                time.Time
	Open                     float64
	High                     float64
	Low                      float64
	Close                    float64
	Volume                   float64
	QuoteAssetVolume         float64
	Trades                   int64
	TakerBuyBaseAssetVolume  float64
	TakerBuyQuoteAssetVolume float64
	IngestedAt               time.Time
	Source                   string
}

type RunInput struct {
	TopN           int
	TimeStart      string
	TimeEnd        string
	VsCurrency     string
	BinanceEnabled bool
}

var supportedIntervalTables = map[string]string{
	"4h": "binance_klines_4h",
	"1d": "binance_klines_1d",
}

func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func IsSupportedInterval(interval string) bool {
	_, ok := supportedIntervalTables[interval]
	return ok
}

func (s *PostgresStore) Init(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, dropSchemaSQL); err != nil {
		return fmt.Errorf("drop schema: %w", err)
	}

	_, err := s.db.ExecContext(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("init schema: %w", err)
	}
	return nil
}

func (s *PostgresStore) StartRun(ctx context.Context, input RunInput) (int64, error) {
	const query = `
		INSERT INTO ingestion_runs (
			started_at, top_n, time_start, time_end, vs_currency, binance_enabled, status
		) VALUES ($1, $2, $3, $4, $5, $6, 'running')
		RETURNING id
	`

	var runID int64
	err := s.db.QueryRowContext(
		ctx,
		query,
		time.Now().UTC(),
		input.TopN,
		input.TimeStart,
		input.TimeEnd,
		strings.ToLower(input.VsCurrency),
		input.BinanceEnabled,
	).Scan(&runID)
	if err != nil {
		return 0, fmt.Errorf("insert ingestion run: %w", err)
	}

	return runID, nil
}

func (s *PostgresStore) FinishRun(ctx context.Context, runID int64, errMessage string) error {
	status := "completed"
	if strings.TrimSpace(errMessage) != "" {
		status = "failed"
	}

	const query = `
		UPDATE ingestion_runs
		SET finished_at = $2, status = $3, error_message = NULLIF($4, '')
		WHERE id = $1
	`

	_, err := s.db.ExecContext(ctx, query, runID, time.Now().UTC(), status, errMessage)
	if err != nil {
		return fmt.Errorf("finish ingestion run: %w", err)
	}
	return nil
}

func (s *PostgresStore) SaveCoinMetadata(ctx context.Context, runID int64, coins []coingecko.Coin, fetchedAt time.Time) error {
	const query = `
		INSERT INTO coins (
			id, symbol, name, market_cap_rank, current_market_cap, current_total_volume,
			current_price, last_updated, metadata_json, fetched_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9::jsonb, $10, $11
		)
		ON CONFLICT (id) DO UPDATE SET
			symbol = EXCLUDED.symbol,
			name = EXCLUDED.name,
			market_cap_rank = EXCLUDED.market_cap_rank,
			current_market_cap = EXCLUDED.current_market_cap,
			current_total_volume = EXCLUDED.current_total_volume,
			current_price = EXCLUDED.current_price,
			last_updated = EXCLUDED.last_updated,
			metadata_json = EXCLUDED.metadata_json,
			fetched_at = EXCLUDED.fetched_at,
			updated_at = EXCLUDED.updated_at
	`

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin metadata tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare metadata stmt: %w", err)
	}
	defer stmt.Close()

	for _, coin := range coins {
		if _, err := stmt.ExecContext(
			ctx,
			coin.ID,
			strings.ToUpper(coin.Symbol),
			coin.Name,
			coin.MarketCapRank,
			coin.MarketCap,
			coin.TotalVolume,
			coin.CurrentPrice,
			coin.LastUpdated.UTC(),
			"{}",
			fetchedAt.UTC(),
			time.Now().UTC(),
		); err != nil {
			return fmt.Errorf("upsert coin metadata: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit metadata tx: %w", err)
	}
	return nil
}

func (s *PostgresStore) SaveBinanceExchangeInfo(ctx context.Context, runID int64, raw []byte, fetchedAt time.Time) error {
	return nil
}

func (s *PostgresStore) SaveBinanceKlinesRaw(ctx context.Context, runID int64, symbolPair string, raw []byte, interval, timeStart, timeEnd string) error {
	return nil
}

func (s *PostgresStore) SaveBinanceRows(ctx context.Context, runID int64, rows []BinanceRow) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	tableName, err := tableNameForInterval(rows[0].Interval)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (
			symbol_pair, interval, open_time, close_time, base_asset, quote_asset, coingecko_id,
			open, high, low, close, volume, quote_asset_volume, trades,
			taker_buy_base_asset_volume, taker_buy_quote_asset_volume, run_id, ingested_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10, $11, $12, $13, $14,
			$15, $16, $17, $18
		)
		ON CONFLICT (symbol_pair, interval, open_time) DO UPDATE SET
			close_time = EXCLUDED.close_time,
			base_asset = EXCLUDED.base_asset,
			quote_asset = EXCLUDED.quote_asset,
			coingecko_id = EXCLUDED.coingecko_id,
			open = EXCLUDED.open,
			high = EXCLUDED.high,
			low = EXCLUDED.low,
			close = EXCLUDED.close,
			volume = EXCLUDED.volume,
			quote_asset_volume = EXCLUDED.quote_asset_volume,
			trades = EXCLUDED.trades,
			taker_buy_base_asset_volume = EXCLUDED.taker_buy_base_asset_volume,
			taker_buy_quote_asset_volume = EXCLUDED.taker_buy_quote_asset_volume,
			run_id = EXCLUDED.run_id,
			ingested_at = EXCLUDED.ingested_at
	`, tableName)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin Binance rows tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("prepare Binance rows stmt: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		if _, err := stmt.ExecContext(
			ctx,
			row.SymbolPair,
			row.Interval,
			row.OpenTime.UTC(),
			row.CloseTime.UTC(),
			row.BaseAsset,
			row.QuoteAsset,
			row.CoinGeckoID,
			row.Open,
			row.High,
			row.Low,
			row.Close,
			row.Volume,
			row.QuoteAssetVolume,
			row.Trades,
			row.TakerBuyBaseAssetVolume,
			row.TakerBuyQuoteAssetVolume,
			runID,
			row.IngestedAt.UTC(),
		); err != nil {
			return 0, fmt.Errorf("insert Binance row: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit Binance rows tx: %w", err)
	}
	return len(rows), nil
}

const schemaSQL = `
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
`

const dropSchemaSQL = `
DROP TABLE IF EXISTS binance_klines_4h;
DROP TABLE IF EXISTS binance_klines_1d;
DROP TABLE IF EXISTS binance_klines;
DROP TABLE IF EXISTS coingecko_market_data;
DROP TABLE IF EXISTS coins;
DROP TABLE IF EXISTS ingestion_runs;
`

func tableNameForInterval(interval string) (string, error) {
	tableName, ok := supportedIntervalTables[interval]
	if !ok {
		return "", fmt.Errorf("unsupported interval for storage table: %s", interval)
	}
	return tableName, nil
}

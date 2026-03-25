package pipeline

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"ai_trading/internal/binance"
	"ai_trading/internal/coingecko"
	"ai_trading/internal/storage"
)

type GeckoClient interface {
	TopCoins(ctx context.Context, topN int, vsCurrency string) ([]coingecko.Coin, []byte, error)
	MarketChartRange(ctx context.Context, coinID, vsCurrency string, from, to time.Time) (coingecko.MarketChart, []byte, error)
}

type BinanceClient interface {
	ExchangeInfo(ctx context.Context) (binance.ExchangeInfo, []byte, error)
	Klines(ctx context.Context, symbol, interval string, start, end time.Time) ([]binance.Kline, []byte, error)
}

type DataStore interface {
	Init(ctx context.Context) error
	StartRun(ctx context.Context, input storage.RunInput) (int64, error)
	FinishRun(ctx context.Context, runID int64, errMessage string) error
	SaveCoinMetadata(ctx context.Context, runID int64, coins []coingecko.Coin, fetchedAt time.Time) error
	SaveCoinGeckoRows(ctx context.Context, runID int64, rows []storage.CoinGeckoRow) (int, error)
	SaveBinanceRows(ctx context.Context, runID int64, rows []storage.BinanceRow) (int, error)
}

type Service struct {
	gecko   GeckoClient
	binance BinanceClient
	store   DataStore
	logger  *log.Logger
}

type RunConfig struct {
	TopN            int
	TimeStart       string
	TimeEnd         string
	VsCurrency      string
	BinanceEnabled  bool
	BinanceInterval string
	BinanceQuote    string
}

type Summary struct {
	CoinsProcessed       int
	CoinGeckoRowsWritten int
	BinancePairsFound    int
	BinanceRowsWritten   int
}

func NewService(gecko GeckoClient, binanceClient BinanceClient, store DataStore, logger *log.Logger) *Service {
	return &Service{
		gecko:   gecko,
		binance: binanceClient,
		store:   store,
		logger:  logger,
	}
}

func (s *Service) Run(ctx context.Context, cfg RunConfig) (Summary, error) {
	from, to, err := parseDateRange(cfg.TimeStart, cfg.TimeEnd)
	if err != nil {
		return Summary{}, err
	}

	if err := s.store.Init(ctx); err != nil {
		return Summary{}, fmt.Errorf("init storage: %w", err)
	}

	runID, err := s.store.StartRun(ctx, storage.RunInput{
		TopN:           cfg.TopN,
		TimeStart:      cfg.TimeStart,
		TimeEnd:        cfg.TimeEnd,
		VsCurrency:     cfg.VsCurrency,
		BinanceEnabled: cfg.BinanceEnabled,
	})
	if err != nil {
		return Summary{}, fmt.Errorf("start ingestion run: %w", err)
	}

	var runErr error
	defer func() {
		message := ""
		if runErr != nil {
			message = runErr.Error()
		}
		if err := s.store.FinishRun(context.Background(), runID, message); err != nil && s.logger != nil {
			s.logger.Printf("failed to finish ingestion run %d: %v", runID, err)
		}
	}()

	fetchedAt := time.Now().UTC()
	coins, rawCoins, err := s.gecko.TopCoins(ctx, cfg.TopN, cfg.VsCurrency)
	_ = rawCoins
	if err != nil {
		runErr = fmt.Errorf("fetch CoinGecko top coins: %w", err)
		return Summary{}, runErr
	}

	if err := s.store.SaveCoinMetadata(ctx, runID, coins, fetchedAt); err != nil {
		runErr = fmt.Errorf("save coin metadata: %w", err)
		return Summary{}, runErr
	}

	var pairMap map[string]string
	summary := Summary{}

	if cfg.BinanceEnabled && s.binance != nil {
		exchangeInfo, rawExchangeInfo, err := s.binance.ExchangeInfo(ctx)
		_ = rawExchangeInfo
		if err != nil {
			runErr = fmt.Errorf("fetch Binance exchange info: %w", err)
			return Summary{}, runErr
		}
		pairMap = buildPairMap(exchangeInfo.Symbols, cfg.BinanceQuote)
	}

	for _, coin := range coins {
		select {
		case <-ctx.Done():
			runErr = ctx.Err()
			return summary, runErr
		default:
		}

		s.logger.Printf("fetching CoinGecko market chart for %s (%s), rank=%d", strings.ToUpper(coin.Symbol), coin.ID, coin.MarketCapRank)

		chart, rawChart, err := s.gecko.MarketChartRange(ctx, coin.ID, cfg.VsCurrency, from, to)
		_ = rawChart
		if err != nil {
			runErr = fmt.Errorf("fetch CoinGecko history for %s: %w", coin.Symbol, err)
			return summary, runErr
		}

		geckoRows := normalizeCoinGecko(coin, chart, cfg.VsCurrency, fetchedAt)
		written, err := s.store.SaveCoinGeckoRows(ctx, runID, geckoRows)
		if err != nil {
			runErr = fmt.Errorf("save CoinGecko rows for %s: %w", coin.Symbol, err)
			return summary, runErr
		}

		summary.CoinsProcessed++
		summary.CoinGeckoRowsWritten += written

		if !cfg.BinanceEnabled || s.binance == nil {
			continue
		}

		pair := pairMap[strings.ToUpper(coin.Symbol)]
		if pair == "" {
			continue
		}

		summary.BinancePairsFound++
		s.logger.Printf("fetching Binance klines for %s", pair)

		klines, rawKlines, err := s.binance.Klines(ctx, pair, cfg.BinanceInterval, from, to)
		_ = rawKlines
		if err != nil {
			runErr = fmt.Errorf("fetch Binance klines for %s: %w", pair, err)
			return summary, runErr
		}

		binanceRows := normalizeBinanceRows(pair, coin, cfg.BinanceQuote, cfg.BinanceInterval, klines, fetchedAt)
		written, err = s.store.SaveBinanceRows(ctx, runID, binanceRows)
		if err != nil {
			runErr = fmt.Errorf("save Binance rows for %s: %w", pair, err)
			return summary, runErr
		}
		summary.BinanceRowsWritten += written
	}

	return summary, nil
}

func normalizeCoinGecko(coin coingecko.Coin, chart coingecko.MarketChart, vsCurrency string, ingestedAt time.Time) []storage.CoinGeckoRow {
	valuesByTime := map[int64]*storage.CoinGeckoRow{}

	getOrCreate := func(ts time.Time) *storage.CoinGeckoRow {
		key := ts.UTC().UnixMilli()
		if row, ok := valuesByTime[key]; ok {
			return row
		}

		row := &storage.CoinGeckoRow{
			CoinID:     coin.ID,
			Symbol:     strings.ToUpper(coin.Symbol),
			Name:       coin.Name,
			VsCurrency: strings.ToLower(vsCurrency),
			Timestamp:  ts.UTC(),
			IngestedAt: ingestedAt.UTC(),
			Source:     "coingecko",
			SourceRank: coin.MarketCapRank,
		}
		valuesByTime[key] = row
		return row
	}

	for _, point := range chart.Prices {
		row := getOrCreate(point.Time)
		row.Price = point.Value
	}
	for _, point := range chart.MarketCaps {
		row := getOrCreate(point.Time)
		row.MarketCap = point.Value
	}
	for _, point := range chart.TotalVolumes {
		row := getOrCreate(point.Time)
		row.TotalVolume = point.Value
	}

	keys := make([]int64, 0, len(valuesByTime))
	for key := range valuesByTime {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	rows := make([]storage.CoinGeckoRow, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, *valuesByTime[key])
	}

	return rows
}

func normalizeBinanceRows(pair string, coin coingecko.Coin, quoteAsset, interval string, klines []binance.Kline, ingestedAt time.Time) []storage.BinanceRow {
	rows := make([]storage.BinanceRow, 0, len(klines))
	for _, kline := range klines {
		rows = append(rows, storage.BinanceRow{
			SymbolPair:               strings.ToUpper(pair),
			BaseAsset:                strings.ToUpper(coin.Symbol),
			QuoteAsset:               strings.ToUpper(quoteAsset),
			CoinGeckoID:              coin.ID,
			Interval:                 interval,
			OpenTime:                 kline.OpenTime.UTC(),
			CloseTime:                kline.CloseTime.UTC(),
			Open:                     kline.Open,
			High:                     kline.High,
			Low:                      kline.Low,
			Close:                    kline.Close,
			Volume:                   kline.Volume,
			QuoteAssetVolume:         kline.QuoteAssetVolume,
			Trades:                   kline.Trades,
			TakerBuyBaseAssetVolume:  kline.TakerBuyBaseAssetVolume,
			TakerBuyQuoteAssetVolume: kline.TakerBuyQuoteAssetVolume,
			IngestedAt:               ingestedAt.UTC(),
			Source:                   "binance",
		})
	}
	return rows
}

func buildPairMap(symbols []binance.SymbolInfo, quoteAsset string) map[string]string {
	targetQuote := strings.ToUpper(quoteAsset)
	pairs := make(map[string]string, len(symbols))
	for _, symbol := range symbols {
		if !symbol.IsSpotTradingAllowed || symbol.Status != "TRADING" {
			continue
		}
		if strings.ToUpper(symbol.QuoteAsset) != targetQuote {
			continue
		}
		base := strings.ToUpper(symbol.BaseAsset)
		if _, exists := pairs[base]; !exists {
			pairs[base] = strings.ToUpper(symbol.Symbol)
		}
	}
	return pairs
}

func parseDateRange(startDate, endDate string) (time.Time, time.Time, error) {
	start, err := time.Parse(time.DateOnly, startDate)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parse start date: %w", err)
	}
	end, err := time.Parse(time.DateOnly, endDate)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parse end date: %w", err)
	}
	return start.UTC(), end.UTC().Add(24*time.Hour - time.Second), nil
}

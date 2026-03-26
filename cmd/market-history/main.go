package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ai_trading/internal/binance"
	"ai_trading/internal/coingecko"
	"ai_trading/internal/config"
	"ai_trading/internal/pipeline"
	"ai_trading/internal/storage"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	cfgPath := flag.String("config", "", "Path to JSON config file")
	topN := flag.Int("top", 0, "Override number of top coins to fetch")
	startDate := flag.String("start", "", "Override start date in YYYY-MM-DD")
	endDate := flag.String("end", "", "Override end date in YYYY-MM-DD")
	outputDir := flag.String("output-dir", "", "Override output directory")
	enableBinance := flag.Bool("binance", false, "Enable Binance OHLCV collection")
	binanceInterval := flag.String("binance-interval", "", "Override Binance interval list, for example: 1h,4h,1d")
	postgresDSN := flag.String("postgres-dsn", "", "Override PostgreSQL DSN")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	if *topN > 0 {
		cfg.TopN = *topN
	}
	if *startDate != "" {
		cfg.TimeStart = *startDate
	}
	if *endDate != "" {
		cfg.TimeEnd = *endDate
	}
	if *outputDir != "" {
		cfg.OutputDir = *outputDir
	}
	if *enableBinance {
		cfg.BinanceEnabled = true
	}
	if *binanceInterval != "" {
		cfg.BinanceInterval = *binanceInterval
	}
	if *postgresDSN != "" {
		cfg.PostgresDSN = *postgresDSN
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("validate config: %v", err)
	}

	logger := log.New(os.Stdout, "[market-history] ", log.LstdFlags)

	geckoClient := coingecko.NewClient(coingecko.ClientOptions{
		APIKey:      cfg.CoinGeckoAPIKey,
		BaseURL:     cfg.CoinGeckoBaseURL,
		HTTPTimeout: time.Duration(cfg.HTTPTimeoutSeconds) * time.Second,
		RateLimit:   time.Duration(cfg.CoinGeckoRequestDelayMS) * time.Millisecond,
		Logger:      logger,
	})

	var binanceClient *binance.Client
	if cfg.BinanceEnabled {
		binanceClient = binance.NewClient(binance.ClientOptions{
			BaseURL:     cfg.BinanceBaseURL,
			HTTPTimeout: time.Duration(cfg.HTTPTimeoutSeconds) * time.Second,
			RateLimit:   time.Duration(cfg.BinanceRequestDelayMS) * time.Millisecond,
			Logger:      logger,
		})
	}

	db, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(context.Background()); err != nil {
		log.Fatalf("ping postgres: %v", err)
	}

	store := storage.NewPostgresStore(db)
	svc := pipeline.NewService(geckoClient, binanceClient, store, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runCfg := pipeline.RunConfig{
		TopN:            cfg.TopN,
		TimeStart:       cfg.TimeStart,
		TimeEnd:         cfg.TimeEnd,
		VsCurrency:      cfg.VsCurrency,
		BinanceEnabled:  cfg.BinanceEnabled,
		BinanceInterval: cfg.BinanceInterval,
		BinanceQuote:    cfg.BinanceQuoteAsset,
	}

	summary, err := svc.Run(ctx, runCfg)
	if err != nil {
		log.Fatalf("pipeline failed: %v", err)
	}

	fmt.Printf(
		"Completed. Coins: %d, Binance pairs: %d, Binance rows: %d, postgres: %s\n",
		summary.CoinsProcessed,
		summary.BinancePairsFound,
		summary.BinanceRowsWritten,
		cfg.PostgresDSN,
	)
}

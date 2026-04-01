package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	defaultOutputDir          = "data"
	defaultTopN               = 100
	defaultVsCurrency         = "usd"
	defaultHTTPTimeoutSeconds = 90
	defaultHistoryStart       = "2013-04-28"
	defaultCoinGeckoBaseURL   = "https://api.coingecko.com/api/v3"
	defaultBinanceBaseURL     = "https://api.binance.com"
	defaultCoinGeckoDelayMS   = 2500
	defaultBinanceDelayMS     = 150
	defaultBinanceInterval    = "4h,1d"
	defaultBinanceQuoteAsset  = "USDT"
	defaultPostgresDSN        = "postgres://admin:admin@localhost:5432/coins?sslmode=disable"
	defaultMinStartDate       = "2017-01-01"
)

type Config struct {
	OutputDir               string `json:"output_dir"`
	TopN                    int    `json:"top_n"`
	TimeStart               string `json:"time_start"`
	TimeEnd                 string `json:"time_end"`
	VsCurrency              string `json:"vs_currency"`
	HTTPTimeoutSeconds      int    `json:"http_timeout_seconds"`
	CoinGeckoAPIKey         string `json:"coingecko_api_key"`
	CoinGeckoBaseURL        string `json:"coingecko_base_url"`
	CoinGeckoRequestDelayMS int    `json:"coingecko_request_delay_ms"`
	BinanceEnabled          bool   `json:"binance_enabled"`
	BinanceBaseURL          string `json:"binance_base_url"`
	BinanceRequestDelayMS   int    `json:"binance_request_delay_ms"`
	BinanceInterval         string `json:"binance_interval"`
	BinanceQuoteAsset       string `json:"binance_quote_asset"`
	PostgresDSN             string `json:"postgres_dsn"`
	MinStartDate            string `json:"min_start_date"`
}

func Load(path string) (Config, error) {
	cfg := Config{
		OutputDir:               envOrDefault("MARKET_DATA_OUTPUT_DIR", defaultOutputDir),
		TopN:                    envIntOrDefault("MARKET_DATA_TOP_N", defaultTopN),
		TimeStart:               envOrDefault("MARKET_DATA_TIME_START", defaultHistoryStart),
		TimeEnd:                 envOrDefault("MARKET_DATA_TIME_END", time.Now().UTC().Format(time.DateOnly)),
		VsCurrency:              envOrDefault("MARKET_DATA_VS_CURRENCY", defaultVsCurrency),
		HTTPTimeoutSeconds:      envIntOrDefault("MARKET_DATA_HTTP_TIMEOUT_SECONDS", defaultHTTPTimeoutSeconds),
		CoinGeckoAPIKey:         strings.TrimSpace(os.Getenv("COINGECKO_API_KEY")),
		CoinGeckoBaseURL:        envOrDefault("COINGECKO_BASE_URL", defaultCoinGeckoBaseURL),
		CoinGeckoRequestDelayMS: envIntOrDefault("COINGECKO_REQUEST_DELAY_MS", defaultCoinGeckoDelayMS),
		BinanceEnabled:          envBoolOrDefault("BINANCE_ENABLED", false),
		BinanceBaseURL:          envOrDefault("BINANCE_BASE_URL", defaultBinanceBaseURL),
		BinanceRequestDelayMS:   envIntOrDefault("BINANCE_REQUEST_DELAY_MS", defaultBinanceDelayMS),
		BinanceInterval:         envOrDefault("BINANCE_INTERVAL", defaultBinanceInterval),
		BinanceQuoteAsset:       envOrDefault("BINANCE_QUOTE_ASSET", defaultBinanceQuoteAsset),
		PostgresDSN:             envOrDefault("POSTGRES_DSN", defaultPostgresDSN),
		MinStartDate:            envOrDefault("MIN_START_DATE", defaultMinStartDate),
	}

	if path == "" {
		return cfg, nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config file: %w", err)
	}

	fileCfg := Config{}
	if err := json.Unmarshal(raw, &fileCfg); err != nil {
		return Config{}, fmt.Errorf("parse config json: %w", err)
	}

	merge(&cfg, fileCfg)
	return cfg, nil
}

func (c Config) Validate() error {
	if c.TopN <= 0 {
		return errors.New("top_n must be > 0")
	}
	if strings.TrimSpace(c.OutputDir) == "" {
		return errors.New("output_dir is required")
	}
	if strings.TrimSpace(c.VsCurrency) == "" {
		return errors.New("vs_currency is required")
	}
	start, err := time.Parse(time.DateOnly, c.TimeStart)
	if err != nil {
		return fmt.Errorf("time_start must be YYYY-MM-DD: %w", err)
	}
	minStart, err := time.Parse(time.DateOnly, c.MinStartDate)
	if err != nil {
		return fmt.Errorf("min_start_date must be YYYY-MM-DD: %w", err)
	}
	end, err := time.Parse(time.DateOnly, c.TimeEnd)
	if err != nil {
		return fmt.Errorf("time_end must be YYYY-MM-DD: %w", err)
	}
	if start.Before(minStart) {
		return fmt.Errorf("time_start %s is earlier than allowed minimum %s", c.TimeStart, c.MinStartDate)
	}
	if start.After(end) {
		return errors.New("time_start must be less than or equal to time_end")
	}
	if c.HTTPTimeoutSeconds <= 0 {
		return errors.New("http_timeout_seconds must be > 0")
	}
	if c.CoinGeckoRequestDelayMS < 0 {
		return errors.New("coingecko_request_delay_ms must be >= 0")
	}
	if c.BinanceRequestDelayMS < 0 {
		return errors.New("binance_request_delay_ms must be >= 0")
	}
	if c.BinanceEnabled && strings.TrimSpace(c.BinanceQuoteAsset) == "" {
		return errors.New("binance_quote_asset is required when Binance is enabled")
	}
	if strings.TrimSpace(c.PostgresDSN) == "" {
		return errors.New("postgres_dsn is required")
	}
	return nil
}

func merge(dst *Config, src Config) {
	if src.OutputDir != "" {
		dst.OutputDir = src.OutputDir
	}
	if src.TopN > 0 {
		dst.TopN = src.TopN
	}
	if src.TimeStart != "" {
		dst.TimeStart = src.TimeStart
	}
	if src.TimeEnd != "" {
		dst.TimeEnd = src.TimeEnd
	}
	if src.VsCurrency != "" {
		dst.VsCurrency = src.VsCurrency
	}
	if src.HTTPTimeoutSeconds > 0 {
		dst.HTTPTimeoutSeconds = src.HTTPTimeoutSeconds
	}
	if src.CoinGeckoAPIKey != "" {
		dst.CoinGeckoAPIKey = src.CoinGeckoAPIKey
	}
	if src.CoinGeckoBaseURL != "" {
		dst.CoinGeckoBaseURL = src.CoinGeckoBaseURL
	}
	if src.CoinGeckoRequestDelayMS >= 0 {
		dst.CoinGeckoRequestDelayMS = src.CoinGeckoRequestDelayMS
	}
	if src.BinanceEnabled {
		dst.BinanceEnabled = true
	}
	if src.BinanceBaseURL != "" {
		dst.BinanceBaseURL = src.BinanceBaseURL
	}
	if src.BinanceRequestDelayMS >= 0 {
		dst.BinanceRequestDelayMS = src.BinanceRequestDelayMS
	}
	if src.BinanceInterval != "" {
		dst.BinanceInterval = src.BinanceInterval
	}
	if src.BinanceQuoteAsset != "" {
		dst.BinanceQuoteAsset = src.BinanceQuoteAsset
	}
	if src.PostgresDSN != "" {
		dst.PostgresDSN = src.PostgresDSN
	}
	if src.MinStartDate != "" {
		dst.MinStartDate = src.MinStartDate
	}
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		return fallback
	}
	return parsed
}

func envBoolOrDefault(key string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	switch value {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

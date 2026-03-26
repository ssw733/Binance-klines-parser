package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxKlinesPerRequest = 1000

type ClientOptions struct {
	BaseURL     string
	HTTPTimeout time.Duration
	RateLimit   time.Duration
	Logger      *log.Logger
}

type Client struct {
	baseURL    string
	httpClient *http.Client
	logger     *log.Logger
	rateLimit  time.Duration
	mu         sync.Mutex
	lastCall   time.Time
}

type ExchangeInfo struct {
	Symbols []SymbolInfo `json:"symbols"`
}

type SymbolInfo struct {
	Symbol               string `json:"symbol"`
	Status               string `json:"status"`
	BaseAsset            string `json:"baseAsset"`
	QuoteAsset           string `json:"quoteAsset"`
	IsSpotTradingAllowed bool   `json:"isSpotTradingAllowed"`
}

type Kline struct {
	OpenTime                 time.Time `json:"open_time"`
	Open                     float64   `json:"open"`
	High                     float64   `json:"high"`
	Low                      float64   `json:"low"`
	Close                    float64   `json:"close"`
	Volume                   float64   `json:"volume"`
	CloseTime                time.Time `json:"close_time"`
	QuoteAssetVolume         float64   `json:"quote_asset_volume"`
	Trades                   int64     `json:"trades"`
	TakerBuyBaseAssetVolume  float64   `json:"taker_buy_base_asset_volume"`
	TakerBuyQuoteAssetVolume float64   `json:"taker_buy_quote_asset_volume"`
}

func NewClient(opts ClientOptions) *Client {
	timeout := opts.HTTPTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	baseURL := strings.TrimRight(opts.BaseURL, "/")
	if baseURL == "" {
		baseURL = "https://api.binance.com"
	}

	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger:    opts.Logger,
		rateLimit: opts.RateLimit,
	}
}

func (c *Client) ExchangeInfo(ctx context.Context) (ExchangeInfo, []byte, error) {
	query := url.Values{}
	query.Set("symbolStatus", "TRADING")

	body, err := c.doRequest(ctx, http.MethodGet, "/api/v3/exchangeInfo", query)
	if err != nil {
		return ExchangeInfo{}, nil, err
	}

	var info ExchangeInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return ExchangeInfo{}, body, fmt.Errorf("decode exchange info: %w", err)
	}

	return info, body, nil
}

func (c *Client) Klines(ctx context.Context, symbol, interval string, start, end time.Time) ([]Kline, []byte, error) {
	current := start.UTC()
	result := make([]Kline, 0, 1024)
	step, err := intervalStep(interval)
	if err != nil {
		return nil, nil, err
	}

	for !current.After(end) {
		windowEnd := current.Add(step * maxKlinesPerRequest).Add(-time.Millisecond)
		if windowEnd.After(end) {
			windowEnd = end.UTC()
		}

		query := url.Values{}
		query.Set("symbol", strings.ToUpper(symbol))
		query.Set("interval", interval)
		query.Set("startTime", strconv.FormatInt(current.UnixMilli(), 10))
		query.Set("endTime", strconv.FormatInt(windowEnd.UnixMilli(), 10))
		query.Set("limit", strconv.Itoa(maxKlinesPerRequest))

		body, err := c.doRequest(ctx, http.MethodGet, "/api/v3/klines", query)
		if err != nil {
			return nil, nil, err
		}

		klines, err := parseKlines(body)
		if err != nil {
			return nil, body, err
		}
		if len(klines) == 0 {
			current = windowEnd.Add(time.Millisecond)
			continue
		}

		result = append(result, klines...)
		current = klines[len(klines)-1].CloseTime.Add(time.Millisecond)
	}

	raw, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return nil, nil, fmt.Errorf("marshal klines: %w", err)
	}

	return result, raw, nil
}

func (c *Client) doRequest(ctx context.Context, method, endpoint string, query url.Values) ([]byte, error) {
	if err := c.waitRateLimit(ctx); err != nil {
		return nil, err
	}

	fullURL := c.baseURL + endpoint
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("binance api error: status=%d body=%s", resp.StatusCode, truncateBody(body))
	}

	return body, nil
}

func (c *Client) waitRateLimit(ctx context.Context) error {
	if c.rateLimit <= 0 {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.lastCall.IsZero() {
		waitFor := c.lastCall.Add(c.rateLimit).Sub(time.Now())
		if waitFor > 0 {
			timer := time.NewTimer(waitFor)
			defer timer.Stop()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
	}

	c.lastCall = time.Now()
	return nil
}

func parseKlines(body []byte) ([]Kline, error) {
	var raw [][]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decode klines response: %w", err)
	}

	klines := make([]Kline, 0, len(raw))
	for _, item := range raw {
		if len(item) < 11 {
			continue
		}

		kline, err := parseKline(item)
		if err != nil {
			return nil, err
		}
		klines = append(klines, kline)
	}

	return klines, nil
}

func parseKline(item []interface{}) (Kline, error) {
	openTime, err := asInt64(item[0])
	if err != nil {
		return Kline{}, fmt.Errorf("parse open time: %w", err)
	}
	open, err := asFloat64(item[1])
	if err != nil {
		return Kline{}, fmt.Errorf("parse open: %w", err)
	}
	high, err := asFloat64(item[2])
	if err != nil {
		return Kline{}, fmt.Errorf("parse high: %w", err)
	}
	low, err := asFloat64(item[3])
	if err != nil {
		return Kline{}, fmt.Errorf("parse low: %w", err)
	}
	closePrice, err := asFloat64(item[4])
	if err != nil {
		return Kline{}, fmt.Errorf("parse close: %w", err)
	}
	volume, err := asFloat64(item[5])
	if err != nil {
		return Kline{}, fmt.Errorf("parse volume: %w", err)
	}
	closeTime, err := asInt64(item[6])
	if err != nil {
		return Kline{}, fmt.Errorf("parse close time: %w", err)
	}
	quoteVolume, err := asFloat64(item[7])
	if err != nil {
		return Kline{}, fmt.Errorf("parse quote volume: %w", err)
	}
	trades, err := asInt64(item[8])
	if err != nil {
		return Kline{}, fmt.Errorf("parse trades: %w", err)
	}
	takerBuyBase, err := asFloat64(item[9])
	if err != nil {
		return Kline{}, fmt.Errorf("parse taker buy base volume: %w", err)
	}
	takerBuyQuote, err := asFloat64(item[10])
	if err != nil {
		return Kline{}, fmt.Errorf("parse taker buy quote volume: %w", err)
	}

	return Kline{
		OpenTime:                 time.UnixMilli(openTime).UTC(),
		Open:                     open,
		High:                     high,
		Low:                      low,
		Close:                    closePrice,
		Volume:                   volume,
		CloseTime:                time.UnixMilli(closeTime).UTC(),
		QuoteAssetVolume:         quoteVolume,
		Trades:                   trades,
		TakerBuyBaseAssetVolume:  takerBuyBase,
		TakerBuyQuoteAssetVolume: takerBuyQuote,
	}, nil
}

func asFloat64(value interface{}) (float64, error) {
	switch typed := value.(type) {
	case string:
		return strconv.ParseFloat(typed, 64)
	case float64:
		return typed, nil
	default:
		return 0, fmt.Errorf("unexpected type %T", value)
	}
}

func intervalStep(interval string) (time.Duration, error) {
	switch interval {
	case "1s":
		return time.Second, nil
	case "1m":
		return time.Minute, nil
	case "3m":
		return 3 * time.Minute, nil
	case "5m":
		return 5 * time.Minute, nil
	case "15m":
		return 15 * time.Minute, nil
	case "30m":
		return 30 * time.Minute, nil
	case "1h":
		return time.Hour, nil
	case "2h":
		return 2 * time.Hour, nil
	case "4h":
		return 4 * time.Hour, nil
	case "6h":
		return 6 * time.Hour, nil
	case "8h":
		return 8 * time.Hour, nil
	case "12h":
		return 12 * time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	case "3d":
		return 72 * time.Hour, nil
	case "1w":
		return 7 * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported interval step: %s", interval)
	}
}

func asInt64(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), nil
	case int64:
		return typed, nil
	default:
		return 0, fmt.Errorf("unexpected type %T", value)
	}
}

func truncateBody(body []byte) string {
	const limit = 400
	if len(body) <= limit {
		return string(body)
	}
	return string(body[:limit]) + "..."
}

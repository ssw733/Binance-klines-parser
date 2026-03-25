package coingecko

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxPerPage = 250
const maxRetries = 5

type ClientOptions struct {
	APIKey      string
	BaseURL     string
	HTTPTimeout time.Duration
	RateLimit   time.Duration
	Logger      *log.Logger
}

type Client struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
	logger     *log.Logger
	rateLimit  time.Duration
	mu         sync.Mutex
	lastCall   time.Time
}

type Coin struct {
	ID            string    `json:"id"`
	Symbol        string    `json:"symbol"`
	Name          string    `json:"name"`
	MarketCapRank int       `json:"market_cap_rank"`
	MarketCap     float64   `json:"market_cap"`
	TotalVolume   float64   `json:"total_volume"`
	CurrentPrice  float64   `json:"current_price"`
	LastUpdated   time.Time `json:"last_updated"`
}

type SeriesPoint struct {
	Time  time.Time `json:"time"`
	Value float64   `json:"value"`
}

type MarketChart struct {
	Prices       []SeriesPoint `json:"prices"`
	MarketCaps   []SeriesPoint `json:"market_caps"`
	TotalVolumes []SeriesPoint `json:"total_volumes"`
}

type marketChartResponse struct {
	Prices       [][]float64 `json:"prices"`
	MarketCaps   [][]float64 `json:"market_caps"`
	TotalVolumes [][]float64 `json:"total_volumes"`
}

func NewClient(opts ClientOptions) *Client {
	timeout := opts.HTTPTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	baseURL := strings.TrimRight(opts.BaseURL, "/")
	if baseURL == "" {
		baseURL = "https://api.coingecko.com/api/v3"
	}

	return &Client{
		apiKey:  strings.TrimSpace(opts.APIKey),
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger:    opts.Logger,
		rateLimit: opts.RateLimit,
	}
}

func (c *Client) TopCoins(ctx context.Context, topN int, vsCurrency string) ([]Coin, []byte, error) {
	coins := make([]Coin, 0, topN)
	page := 1

	for len(coins) < topN {
		remaining := topN - len(coins)
		perPage := remaining
		if perPage > maxPerPage {
			perPage = maxPerPage
		}

		query := url.Values{}
		query.Set("vs_currency", strings.ToLower(vsCurrency))
		query.Set("order", "market_cap_desc")
		query.Set("per_page", strconv.Itoa(perPage))
		query.Set("page", strconv.Itoa(page))
		query.Set("sparkline", "false")

		body, err := c.doRequest(ctx, http.MethodGet, "/coins/markets", query)
		if err != nil {
			return nil, nil, err
		}

		var pageCoins []Coin
		if err := json.Unmarshal(body, &pageCoins); err != nil {
			return nil, nil, fmt.Errorf("decode coins/markets response: %w", err)
		}
		if len(pageCoins) == 0 {
			break
		}

		coins = append(coins, pageCoins...)
		page++
	}

	if len(coins) > topN {
		coins = coins[:topN]
	}

	raw, err := json.MarshalIndent(coins, "", "  ")
	if err != nil {
		return nil, nil, fmt.Errorf("marshal top coins: %w", err)
	}

	return coins, raw, nil
}

func (c *Client) MarketChartRange(ctx context.Context, coinID, vsCurrency string, from, to time.Time) (MarketChart, []byte, error) {
	from, to, err := c.normalizeRange(from, to)
	if err != nil {
		return MarketChart{}, nil, err
	}

	query := url.Values{}
	query.Set("vs_currency", strings.ToLower(vsCurrency))
	query.Set("from", strconv.FormatInt(from.UTC().Unix(), 10))
	query.Set("to", strconv.FormatInt(to.UTC().Unix(), 10))

	endpoint := path.Join("/coins", coinID, "market_chart/range")
	body, err := c.doRequest(ctx, http.MethodGet, endpoint, query)
	if err != nil {
		return MarketChart{}, nil, err
	}

	var rawResp marketChartResponse
	if err := json.Unmarshal(body, &rawResp); err != nil {
		return MarketChart{}, body, fmt.Errorf("decode market chart response: %w", err)
	}

	return MarketChart{
		Prices:       parseSeries(rawResp.Prices),
		MarketCaps:   parseSeries(rawResp.MarketCaps),
		TotalVolumes: parseSeries(rawResp.TotalVolumes),
	}, body, nil
}

func (c *Client) normalizeRange(from, to time.Time) (time.Time, time.Time, error) {
	if strings.Contains(c.baseURL, "pro-api.coingecko.com") {
		return from, to, nil
	}

	cutoff := time.Now().UTC().AddDate(-1, 0, 0)
	if to.Before(cutoff) {
		return time.Time{}, time.Time{}, fmt.Errorf(
			"CoinGecko public/demo API only supports the last 365 days; requested end date %s is older than cutoff %s",
			to.UTC().Format(time.DateOnly),
			cutoff.Format(time.DateOnly),
		)
	}

	if from.Before(cutoff) {
		if c.logger != nil {
			c.logger.Printf(
				"CoinGecko public/demo API supports only the last 365 days, clamping start date from %s to %s",
				from.UTC().Format(time.DateOnly),
				cutoff.Format(time.DateOnly),
			)
		}
		from = cutoff
	}

	return from, to, nil
}

func (c *Client) doRequest(ctx context.Context, method, endpoint string, query url.Values) ([]byte, error) {
	fullURL := c.baseURL + endpoint
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := c.waitRateLimit(ctx); err != nil {
			return nil, err
		}

		req, err := http.NewRequestWithContext(ctx, method, fullURL, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}

		req.Header.Set("Accept", "application/json")
		if c.apiKey != "" {
			req.Header.Set("x-cg-demo-api-key", c.apiKey)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("http request: %w", err)
			if attempt == maxRetries {
				return nil, lastErr
			}
			if err := c.sleepWithContext(ctx, backoffDuration(attempt)); err != nil {
				return nil, err
			}
			continue
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("read response: %w", readErr)
			if attempt == maxRetries {
				return nil, lastErr
			}
			if err := c.sleepWithContext(ctx, backoffDuration(attempt)); err != nil {
				return nil, err
			}
			continue
		}

		if resp.StatusCode < 300 {
			return body, nil
		}

		lastErr = fmt.Errorf("coingecko api error: status=%d body=%s", resp.StatusCode, truncateBody(body))
		if !shouldRetry(resp.StatusCode) || attempt == maxRetries {
			return nil, lastErr
		}

		retryAfter := retryAfterDuration(resp.Header.Get("Retry-After"))
		if retryAfter <= 0 {
			retryAfter = backoffDuration(attempt)
		}
		if c.logger != nil {
			c.logger.Printf("CoinGecko request hit status %d, retrying in %s", resp.StatusCode, retryAfter)
		}
		if err := c.sleepWithContext(ctx, retryAfter); err != nil {
			return nil, err
		}
	}

	return nil, lastErr
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

func parseSeries(values [][]float64) []SeriesPoint {
	points := make([]SeriesPoint, 0, len(values))
	for _, item := range values {
		if len(item) < 2 {
			continue
		}

		timestampMs := int64(item[0])
		points = append(points, SeriesPoint{
			Time:  time.UnixMilli(timestampMs).UTC(),
			Value: item[1],
		})
	}
	return points
}

func truncateBody(body []byte) string {
	const limit = 400
	if len(body) <= limit {
		return string(body)
	}
	return string(body[:limit]) + "..."
}

func shouldRetry(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || statusCode >= 500
}

func retryAfterDuration(header string) time.Duration {
	if strings.TrimSpace(header) == "" {
		return 0
	}

	if seconds, err := strconv.Atoi(header); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}

	if when, err := http.ParseTime(header); err == nil {
		waitFor := time.Until(when)
		if waitFor > 0 {
			return waitFor
		}
	}

	return 0
}

func backoffDuration(attempt int) time.Duration {
	backoff := 5 * time.Second
	for i := 0; i < attempt; i++ {
		backoff *= 2
		if backoff >= 60*time.Second {
			return 60 * time.Second
		}
	}
	return backoff
}

func (c *Client) sleepWithContext(ctx context.Context, waitFor time.Duration) error {
	timer := time.NewTimer(waitFor)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

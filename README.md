# AI Trading Market Data Collector

Go-проект для загрузки полной исторической выборки по TOP N криптомонетам с сохранением в `PostgreSQL`.

Основная схема:

- `CoinGecko` только для списка топ-монет и метаданных
- `Binance` как источник исторических свечей `OHLCV`
- `PostgreSQL` для таблиц по интервалам и метаданных запусков

## Что именно собираем

### CoinGecko

Через `coins/markets`:

- top N монет по market cap
- `symbol`, `name`, `rank`, `market cap`, `volume`

Исторические ряды из CoinGecko больше не запрашиваются.

### Binance

Если включен `binance_enabled`, проект:

1. Загружает `exchangeInfo`
2. Ищет spot-пары с нужной котировкой, например `USDT`
3. Для найденных пар тянет `klines` пагинацией по всему диапазону дат
4. Складывает свечи в отдельные таблицы по интервалам

## База данных

Основные таблицы:

- `ingestion_runs`
- `coins`
- `binance_klines_1h`
- `binance_klines_4h`
- `binance_klines_1d`

SQL-схема лежит в [001_init.sql](/home/alexander/AI_trading/sql/001_init.sql), а приложение автоматически дропает свои рабочие таблицы и создает их заново перед запуском.

## Запуск

Основной запуск:

```bash
go run ./cmd/market-history -top 50 -start 2018-01-01 -end 2026-03-26 -binance -binance-interval 1h,4h,1d \
  -postgres-dsn 'postgres://admin:admin@localhost:5432/coins?sslmode=disable'
```

Через конфиг:

```bash
go run ./cmd/market-history -config ./config.example.json
```

## Что сохраняется в Postgres

`coins`:

- `id`
- `symbol`
- `name`
- `market_cap_rank`
- `current_market_cap`
- `current_total_volume`
- `current_price`

`binance_klines_1h`, `binance_klines_4h`, `binance_klines_1d`:

- `symbol_pair`
- `interval`
- `open_time`
- `close_time`
- `base_asset`
- `quote_asset`
- `coingecko_id`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `quote_asset_volume`
- `trades`
- `taker_buy_base_asset_volume`
- `taker_buy_quote_asset_volume`
- `run_id`

## Практические замечания

- `CoinGecko` используется только для списка монет и метаданных, поэтому лимиты мешают заметно меньше.
- Не каждая топ-монета торгуется на Binance в паре `USDT`, поэтому часть активов останется только в таблице `coins`.
- `BINANCE_INTERVAL` поддерживает список интервалов через запятую: `1h,4h,1d`
- Для хранения сейчас поддерживаются интервалы `1h`, `4h`, `1d`
- Есть защита от слишком ранней даты старта: по умолчанию `MIN_START_DATE=2017-01-01`

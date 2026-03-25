# AI Trading Market Data Collector

Go-проект для загрузки полной исторической выборки по TOP N криптомонетам без платного CoinMarketCap с сохранением в `PostgreSQL`.

Основная схема:

- `CoinGecko` для списка топ-монет и исторических рядов `price / market_cap / total_volume`
- `Binance` как опциональный источник биржевых свечей `OHLCV` для пар вроде `BTCUSDT`
- `PostgreSQL` для нормализованных временных рядов и метаданных загрузок

## Что именно собираем

### CoinGecko

Через `coins/markets`:

- top N монет по market cap
- symbol, name, rank, market cap, volume

Через `coins/{id}/market_chart/range`:

- исторический `price`
- исторический `market_cap`
- исторический `total_volume`

Это хороший бесплатный базовый датасет для обучения модели на уровне всего рынка.

### Binance

Если включен `binance_enabled`, проект дополнительно:

1. Загружает `exchangeInfo`
2. Ищет spot-пары с нужной котировкой, например `USDT`
3. Для найденных пар тянет `klines` пагинацией по всему диапазону дат

Это дает уже настоящие биржевые свечи `open/high/low/close/volume`.

## Структура

```text
.
├── cmd/market-history
├── internal/binance
├── internal/coingecko
├── internal/config
├── internal/pipeline
├── internal/storage
├── config.example.json
└── .env.example
```

## База данных

Основные таблицы:

- `ingestion_runs`
- `coins`
- `coingecko_market_data`
- `binance_klines`

SQL-схема лежит в [001_init.sql](/home/alexander/AI_trading/sql/001_init.sql), а приложение умеет автоматически создать таблицы при запуске.
Перед каждым запуском приложение дропает свои рабочие таблицы и создает их заново, поэтому база для этого парсера всегда начинается с чистого состояния.

Для локального старта добавлен [docker-compose.yml](/home/alexander/AI_trading/docker-compose.yml):

```bash
docker compose up -d
```

## Запуск

Минимальный запуск только с CoinGecko:

```bash
go run ./cmd/market-history -top 50 -start 2018-01-01 -end 2026-03-25 \
  -postgres-dsn 'postgres://admin:admin@localhost:5432/coins?sslmode=disable'
```

С Binance:

```bash
go run ./cmd/market-history -top 50 -start 2018-01-01 -end 2026-03-25 -binance -binance-interval 1d \
  -postgres-dsn 'postgres://admin:admin@localhost:5432/coins?sslmode=disable'
```

Через конфиг:

```bash
go run ./cmd/market-history -config ./config.example.json
```

## Что сохраняется в Postgres

`coingecko_market_data`:

- `coin_id`
- `symbol`
- `name`
- `vs_currency`
- `ts`
- `price`
- `market_cap`
- `total_volume`
- `source_rank`
- `run_id`

`binance_klines`:

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

- У `CoinGecko` бесплатный план ограничен rate limit и месячной квотой, поэтому в конфиге уже заложена пауза между запросами.
- `CoinGecko` и `Binance` дают разные типы данных: агрегированный рыночный ряд и данные конкретной биржи. Для обучения это скорее плюс, чем минус.
- Не каждая топ-монета торгуется на Binance в паре `USDT`, поэтому часть активов будет только в `CoinGecko`.
- `TOP N` фиксируется на дату запуска. Если позже понадобится исторический состав рынка по датам, это будет отдельный snapshot-пайплайн.
- Запись идет через `upsert`, поэтому повторный запуск не создает дубликаты по ключевым временным точкам.

## Что логично делать дальше

Следующий сильный шаг для проекта:

1. добавить инкрементальные дозагрузки вместо полной перезаписи CSV
2. добавить DuckDB/Parquet как формат для обучения
3. добавить feature engineering поверх `silver` слоя

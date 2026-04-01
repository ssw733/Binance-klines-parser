# AI Trading Market Data Collector

Go-проект для загрузки полной исторической выборки по TOP N криптомонетам с сохранением в `PostgreSQL`.

Основная схема:

- `CoinGecko` только для списка топ-монет и метаданных
- `Binance` как источник исторических свечей `OHLCV`
- `PostgreSQL` для таблиц по интервалам и метаданных запусков

## Что именно собираем

### CoinGecko

Используется только как fallback, если Binance-режим выключен.

### Binance

Если включен `binance_enabled`, проект:

1. Загружает `exchangeInfo`
2. Берет все spot-монеты Binance с нужной котировкой, например `USDT`
3. Для найденных пар тянет `klines` пагинацией по всему диапазону дат
4. Складывает свечи в отдельные таблицы по интервалам

## База данных

Основные таблицы:

- `ingestion_runs`
- `coins`
- `binance_klines_4h`
- `binance_klines_1d`

SQL-схема лежит в [001_init.sql](/home/alexander/AI_trading/sql/001_init.sql), а приложение автоматически дропает свои рабочие таблицы и создает их заново перед запуском.

## Запуск

Основной запуск:

```bash
go run ./cmd/market-history -start 2018-01-01 -end 2026-04-01 -binance -binance-interval 4h,1d \
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

`binance_klines_4h`, `binance_klines_1d`:

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
- В `Binance`-режиме universe монет берется не из `top`, а из всех доступных Binance spot-пар с нужной котировкой.
- `BINANCE_INTERVAL` поддерживает список интервалов через запятую: `4h,1d`
- Для хранения сейчас поддерживаются интервалы `4h`, `1d`
- Есть защита от слишком ранней даты старта: по умолчанию `MIN_START_DATE=2017-01-01`

# Serverless Trading System (News -> Sentiment -> Paper Trades)

This project implements the requested pipeline using AWS serverless components and Python 3.12.

Architecture:
1. EventBridge cron triggers `news_fetcher` on weekdays at `09:30` (UTC by default).
2. `news_fetcher` fetches recent news from NewsAPI for the top 10 symbols and sends the payload to an SQS queue.
3. `sentiment_analyzer` consumes the SQS message, calls Claude (Anthropic) to score sentiment, and stores results + backtesting price context in DynamoDB.
4. `sentiment_analyzer` then invokes `trade_executor` synchronously.
5. `trade_executor` loads the symbols for the given `run_id`, then for each symbol **queries recent `SENTIMENT` rows on the `TickerTimestampIndex` GSI** (`gsi_pk` = symbol, `gsi_sk` = time) to build **news rows** for the registered strategy. It calls **`strategy.check_live_signal(...)`** and places paper trades via Alpaca (idempotent per symbol per run). New `TRADE` items are written with **`status=OPEN`**, **`hold_minutes`**, **`strategy_name`**, and **`exit_type`** for the exit manager.
6. **`exit_manager`** (Lambda 4) runs on **EventBridge `rate(1 minute)`**: scans for **`TRADE` + `status=OPEN`**, then exits per **`exit_type`** — **`fixed_time`** (after `submitted_at + hold_minutes`), **`end_of_day`** (within `EXIT_EOD_WINDOW_MINUTES` of Alpaca close), or **`signal_flip`** (strategy **`check_live_signal`** side vs entry side). It submits a flattening market order on Alpaca and sets **`status=CLOSED`**, **`realized_pnl_usd`** (position `unrealized_pl` snapshot at exit), **`exit_alpaca_order_id`**, **`closed_at`**, **`exit_reason`**.
7. DynamoDB Streams triggers `sentiment_to_s3_parquet`, which appends new `SENTIMENT` rows into an S3 Parquet lake.
8. A weekly EventBridge Scheduler job runs an ECS Fargate VectorBT backtest task to evaluate threshold quality.

## Repo Layout

- `lambdas/news_fetcher/` (NewsAPI -> SQS)
- `lambdas/sentiment_analyzer/` (SQS -> Claude sentiment + historical price context -> DynamoDB -> invoke trade executor)
- `lambdas/trade_executor/` (DynamoDB sentiment + strategy config -> Alpaca paper trades)
- `lambdas/exit_manager/` (scheduled: close OPEN `TRADE` rows per `exit_type`)
- `lambdas/sentiment_to_s3_parquet/` (DynamoDB stream -> S3 Parquet sink via awswrangler)
- `analysis/backtest_report.py` (scan SENTIMENT data + forward returns report)
- `scripts/dynamodb_native_export.py` (enable PITR + start native export to S3)
- `backtester/` (Dockerized VectorBT engine + ECS task definition template)
- `strategies/` (shared **BaseStrategy** + **MorningSentimentStrategy** / decay math for backtest + live)
- `template.yaml` (AWS SAM IaC)
- `.env.example` (local environment variable template)

## Prerequisites

- AWS SAM CLI installed (`sam`)
- AWS credentials configured (e.g. `aws configure`)
- Access to:
  - NewsAPI
  - Anthropic (Claude API)
  - Alpaca paper trading

## Configuration

Secrets are read from AWS Secrets Manager via SAM dynamic references.

Create one secret per API credential (example commands):

```bash
aws secretsmanager create-secret \
  --name trading/newsapi \
  --secret-string '{"api_key":"YOUR_NEWS_API_KEY"}'

aws secretsmanager create-secret \
  --name trading/anthropic \
  --secret-string '{"api_key":"YOUR_ANTHROPIC_API_KEY"}'

aws secretsmanager create-secret \
  --name trading/alpaca-api-key \
  --secret-string '{"api_key":"YOUR_ALPACA_API_KEY"}'

aws secretsmanager create-secret \
  --name trading/alpaca-secret-key \
  --secret-string '{"secret_key":"YOUR_ALPACA_SECRET_KEY"}'
```

Then pass these secret names/ARNs as SAM parameters:
- `NewsApiSecretId`
- `AnthropicApiSecretId`
- `AlpacaApiKeySecretId`
- `AlpacaSecretKeySecretId`

Non-secret defaults (thresholds, schedule time, symbol list, etc.) are parameters in `template.yaml` and can be overridden.

Additional parameters for backtesting pipeline:
- `BacktestLakeBucketName` (S3 bucket for Parquet lake and exports)
- `BacktestLakePrefix` (S3 prefix for buffered parquet data)
- `BacktestEcrImageUri` (ECR image URI for Fargate backtester)
- `BacktestSymbol` (symbol used by scheduled threshold optimization)
- `BacktestStrategyPromotionEnabled` (`true`|`false`) — when `true`, after a Sharpe improvement the backtester runs **hybrid promotion gates** and may update `STRATEGY#<SYMBOL>/LATEST`
- `StrategyPromotionAlertTopicArn` — optional SNS topic for promotion approved / rejected notifications
- `EnableSentimentParquetSink` (`true`/`false`) — only enable when the stream→Parquet Lambda can be deployed (large dependency zip); otherwise keep `false` for lean deploys

EventBridge schedule:
- The cron expression uses UTC by default.
- If you need a different timezone, adjust `ScheduleHour` / `ScheduleMinute` parameters accordingly.

## Deploy

1. Build:

```bash
sam build
```

2. Deploy (example):

```bash
sam deploy --guided
```

When prompted, provide the required secret id parameters:
- `NewsApiSecretId` (example: `trading/newsapi`)
- `AnthropicApiSecretId` (example: `trading/anthropic`)
- `AlpacaApiKeySecretId` (example: `trading/alpaca-api-key`)
- `AlpacaSecretKeySecretId` (example: `trading/alpaca-secret-key`)

Or deploy with explicit parameter overrides, for example:

```bash
sam deploy \
  --stack-name trading-system \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    NewsApiSecretId="trading/newsapi" \
    AnthropicApiSecretId="trading/anthropic" \
    AlpacaApiKeySecretId="trading/alpaca-api-key" \
    AlpacaSecretKeySecretId="trading/alpaca-secret-key"
```

Force refresh tip:
- If Secrets Manager values changed but `sam deploy` says "No changes to deploy", pass a new `ForceRefreshToken` value to force Lambda env refresh.

```bash
sam deploy \
  --stack-name trading-system \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    NewsApiSecretId="trading/newsapi" \
    AnthropicApiSecretId="trading/anthropic" \
    AlpacaApiKeySecretId="trading/alpaca-api-key" \
    AlpacaSecretKeySecretId="trading/alpaca-secret-key" \
    ForceRefreshToken="2026-03-19T20:10:00Z"
```

## Sentiment Data Model

`sentiment_analyzer` stores one `SENTIMENT#<symbol>` item per symbol/run with fields for backtesting:

- Core sentiment: `sentiment_label`, `sentiment_score`, `rationale`
- Timing: `news_published_at`, `market_price_timestamp`, `analyzed_at`, `triggered_at`
- Price context: `market_price_at_news`, `market_session`, `is_regular_hours`, `market_price_lookup_start_at`
- **GSI (time-series queries):** `gsi_pk` (uppercase symbol), `gsi_sk` (ISO timestamp for query range — aligns with `news_published_at` / analysis time). Used by `trade_executor` on **`TickerTimestampIndex`**.

Market-session handling:

- If `publishedAt` is in regular hours (09:30-16:00 ET), price lookup starts at that timestamp.
- If `publishedAt` is outside regular hours, lookup shifts to the next regular open (09:30 ET on next trading weekday).
- `sentiment_analyzer` first tries Alpaca market calendar (`/v2/calendar`) to handle holidays/closures; if unavailable, it falls back to weekday-only next-open logic.

## Strategy-Based Trading

`trade_executor` loads **strategy config** from DynamoDB, builds a **`LiveSentimentStrategy`** via `get_strategy_instance(...)`, then calls **`check_live_signal(current_prices, current_news, ...)`** where:

- **`current_news`**: list of row dicts (`sentiment_score`, `news_published_at`, `analyzed_at`) from the last 7d of `SENTIMENT` items (same logical columns as a pandas DataFrame).
- **`current_prices`**: list of recent 1m bar dicts from Alpaca (`timestamp`, OHLCV); empty if data API creds are missing or the request fails (strategy is news-driven today).

If `check_live_signal` returns a side, the handler calls **`_place_order`** (Alpaca) and writes the `TRADE` row as before.

### Time-decayed sentiment (live)

For each symbol in the current `run_id`, the executor **does not** use only that run’s single `SENTIMENT` row. It **queries `TickerTimestampIndex`** for all `SENTIMENT` items for that symbol and computes a **weighted average** with exponential decay \(w = e^{-\lambda t}\), \(t\) = age in hours:

- **Primary window:** last **24 hours**, \(\lambda = 0.1\)
- **Fallback:** if empty, last **7 days**, \(\lambda = 0.5\)
- **No articles:** signal **0**

Published times are parsed to **UTC** for age (`news_published_at` when present). Skip / threshold logs are emitted from inside the strategy’s `check_live_signal`.

### Gates (inside `check_live_signal`, after the decayed signal is computed)

1. **Magnitude:** `abs(decayed_signal) >= max(abs(SENTIMENT_BUY_THRESHOLD), abs(SENTIMENT_SELL_THRESHOLD))` — otherwise skip (too weak to treat as actionable).
2. **Strategy row** (see below): `is_active` and `decayed_signal >= optimized_threshold` (long-biased gate before `_decide_side`).
3. **Direction:** `SENTIMENT_BUY_THRESHOLD` / `SENTIMENT_SELL_THRESHOLD` (+ optional shorts) choose BUY vs SELL.
4. **Close-only (optional):** if `TradeOnlyAtClose=true`, trading is only in the last `CloseWindowMinutes` of the regular session (default **5** minutes before close).

Expected strategy item in DynamoDB:

- PK (`run_id`): `STRATEGY#<SYMBOL>`
- SK (`sort_key`): `LATEST`
- Fields:
  - `is_active` (Boolean)
  - `optimized_threshold` (Decimal)
  - `strategy_name` (String, e.g. `Sentiment_V1`) — must exist in `STRATEGY_MAP`
  - `exit_type` (String: `fixed_time` | `end_of_day` | `signal_flip`)
  - `hold_minutes` (Number) — used by `exit_manager` for `fixed_time`

If strategy config is missing, defaults are conservative (`is_active=false`, threshold=`1.0`), so no accidental trade.

### `TRADE` items (written by `trade_executor`, closed by `exit_manager`)

- `item_type`: `TRADE`
- `status`: `OPEN` → `CLOSED`
- `exit_type`, `hold_minutes`, `strategy_name` (copied from strategy at entry)
- On close: `closed_at`, `exit_reason`, `realized_pnl_usd` (Alpaca position `unrealized_pl` snapshot before flatten), `exit_alpaca_order_id`

**Note:** `exit_manager` **scans** `item_type=TRADE` and filters `status=OPEN` in code (no extra GSI). For large tables, consider a dedicated GSI later.

## How retries & safety work

- SQS queue uses a DLQ with `maxReceiveCount: 3`.
- Each Lambda uses exponential backoff retry logic for its external API calls:
  - NewsAPI (news_fetcher)
  - Claude (sentiment_analyzer)
  - Alpaca (trade_executor, exit_manager)
- DynamoDB writes are idempotent per `run_id` and symbol:
  - Sentiment items use `batch_writer(overwrite_by_pkeys=["run_id","sort_key"])` for throughput and deterministic upsert on retries.
  - Trade execution writes `TRADE#<symbol>` records conditionally; if a trade record already exists, the executor skips it.

## CloudWatch P&L metrics (trade executor)

After each successful `trade_executor` run, the Lambda logs an **Alpaca account snapshot** and (by default) calls **`PutMetricData`** so you can chart P&L in a dashboard.

- **Namespace:** `Trading/Paper` (override with SAM parameter `CloudWatchMetricNamespace`).
- **Metric names:** PascalCase from the snapshot, e.g. `EquityUsd`, `DayPlUsd`, `DayPlPct`, `UnrealizedPlUsd`, `AccountStatusCode`.
- **Dimension:** `PaperTrading` = `true` or `false` (from `ALPACA_PAPER`).
- **Disable publishing:** deploy with `PublishCloudWatchMetrics=false` (metrics still appear in logs as structured fields).

**Add a dashboard widget:** CloudWatch → Dashboards → Add widget → Line → Metrics → Custom namespaces → choose your namespace → select metrics (e.g. `EquityUsd`, `DayPlPct`).

## Paper Trading Notes

- This uses Alpaca “paper trading” by default (`ALPACA_PAPER=true` in the SAM template).
- By default, shorts are disabled. Enable with `EnableShorts=true` if your paper account supports it.
- Trade sizing uses either:
  - `TRADE_NOTIONAL_USD`, or
  - `TRADE_QTY` (leave `TradeQty` empty to use notional).
- Close-only execution controls:
  - `TradeOnlyAtClose` (default: `true`)
  - `CloseWindowMinutes` (default: `5`)

## Optional Real-Time Rule (disabled)

`template.yaml` includes an optional `RealTimeTradingRule` (`rate(1 minute)`), currently:

- `State: DISABLED`

Enable only when you intentionally want near-real-time triggering.

## DynamoDB -> S3 Data Lake

`template.yaml` now enables:
- DynamoDB PITR (`TradingTable`)
- DynamoDB stream (`NEW_IMAGE`)
- `SentimentToS3ParquetFunction` stream consumer

The stream consumer writes appended parquet files partitioned by date under:
- `s3://<BacktestLakeBucketName>/<BacktestLakePrefix>/`

## Native DynamoDB Export (on-demand)

Use the script to enable PITR and trigger a native export:

```bash
python3 scripts/dynamodb_native_export.py \
  --table-name TradingNewsSentiment \
  --s3-bucket <your-lake-bucket> \
  --s3-prefix dynamodb-exports/tradingnews
```

Notes:
- Native export format is DynamoDB JSON.
- `backtester/backtest_engine.py` supports this format via `S3_DDB_EXPORT_PREFIX`.

## Weekly Fargate Backtester

Infra added in `template.yaml`:
- ECS cluster: `BacktestCluster`
- Task definition: `BacktestTaskDefinition` (`2 vCPU`, `4GB`)
- Scheduler: `WeeklyBacktestSchedule` (`Sunday 23:00 UTC`, `FlexibleTimeWindow: OFF`)

The schedule target uses `ecs:RunTask` and requires real VPC IDs. Replace placeholders before deploy:
- `subnet-CHANGE_ME_A`, `subnet-CHANGE_ME_B`, `sg-CHANGE_ME`

### Build and push image (example)

```bash
docker build -f backtester/Dockerfile -t trading-backtester:latest .
# tag + push to ECR, then set BacktestEcrImageUri in sam deploy
```

### Backtester inputs

`backtester/backtest_engine.py` accepts either:
- `S3_PARQUET_PATH` (preferred, parquet dataset), or
- `S3_DDB_EXPORT_PREFIX` (native export path; unmarshalled in code)

For historical bars reuse, set:
- `S3_BARS_CACHE_PREFIX` (example: `s3://<BacktestLakeBucketName>/bars-cache`)

Default in SAM task definition:
- `S3_BARS_CACHE_PREFIX=s3://<BacktestLakeBucketName>/bars-cache`

Cache behavior:
- The backtester checks weekly symbol bars parquet in S3 first.
- On cache miss, it fetches bars from Alpaca, writes parquet to S3 cache, and reuses it on future runs.

**Strategy Pattern layout (`backtester/backtest_engine.py`):**
- **`BaseStrategy`**: abstract `generate_signals(price_df, news_df) -> (entries, exits)` returning boolean pandas Series.
- **`MorningSentimentStrategy`**: concrete strategy with existing 9:30 ET entries + 24h decayed sentiment (7d fallback) logic moved into the class.
- **`exit_type`** supported by constructor: `fixed_time`, `end_of_day`, `signal_flip`.
- **Threshold optimization** is generic: runner sets threshold, strategy generates signals, VectorBT evaluates.

**Backtest env knobs:**
- `BACKTEST_HOLD_MINUTES` (used by `fixed_time`)
- `BACKTEST_EXIT_TYPE` (`fixed_time`, `end_of_day`, `signal_flip`)
- `BACKTEST_MIN_SHARPE_IMPROVEMENT`

It writes candidate results to DynamoDB as:
- `run_id=BACKTEST#<SYMBOL>`
- `sort_key=<UTC timestamp>`
- `item_type=BACKTEST`

**Hybrid promotion to `STRATEGY#<SYMBOL>/LATEST` (optional):** Set `STRATEGY_PROMOTION_ENABLED=true` on the ECS task (SAM: `BacktestStrategyPromotionEnabled=true`). After a **Sharpe improvement**, the job still writes `BACKTEST#…` first, then evaluates four gates on the **candidate threshold** and **symbol’s news sample** in the backtest window:

| Gate | Rule |
|------|------|
| Min threshold | `new > 0.60` |
| Max threshold | `new < 0.90` |
| Max jump | if a prior `optimized_threshold` exists in DDB: `abs(new - old) < 0.15` |
| Sample size | `articles > 20` (row count of the symbol’s `news_df`) |

If **any** gate fails: **no** update to `STRATEGY#LATEST`, structured **error logs**, and **SNS** to `STRATEGY_PROMOTION_ALERT_SNS_TOPIC_ARN` when set. Tunables: `STRATEGY_PROMOTION_MIN_THRESHOLD`, `STRATEGY_PROMOTION_MAX_THRESHOLD`, `STRATEGY_PROMOTION_MAX_JUMP`, `STRATEGY_PROMOTION_MIN_ARTICLES`. Set `STRATEGY_PROMOTE_SET_ACTIVE=true` only if you want promotion to force `is_active=true`; otherwise existing `is_active` is preserved (new rows default to inactive).

**Bootstrap `STRATEGY#…/LATEST` (Phase 1):**

```bash
pip install boto3 pydantic
export DYNAMODB_TABLE_NAME=TradingNewsSentiment
python3 scripts/seed_strategy_latest.py --symbols TSLA
# optional: --active --threshold 0.65
```

## Local Development (optional)

1. Copy:

```bash
cp .env.example .env
```

2. Update API keys in `.env`.

SAM local can load environment variables depending on how you invoke it; the Lambdas also attempt a best-effort `.env` load when `python-dotenv` is installed.

## GitHub Actions (CI / deploy)

Two workflows live under `.github/workflows/`:

| Workflow | When it runs | What it does |
|----------|----------------|---------------|
| **CI** (`ci.yml`) | Every push / PR to `main` or `master` | `sam validate`, `sam build`, unit tests — **no AWS credentials** needed. |
| **Deploy** (`deploy.yml`) | **Actions → Deploy → Run workflow** (manual) | `sam build` + `sam deploy` using AWS **OIDC role assumption** (no long-lived access keys). |

### One-time setup for deploy

1. In AWS IAM, create a role trusted by GitHub OIDC provider (`token.actions.githubusercontent.com`) with trust policy restricting your repo/branch (example below).
2. Attach deploy permissions to that role (CloudFormation, Lambda, IAM pass role if needed, S3 artifacts, etc. — same permissions you use locally for `sam deploy`).
3. In GitHub: **Settings → Secrets and variables → Actions → Variables**:
   - `AWS_GITHUB_OIDC_ROLE_ARN` = `arn:aws:iam::<account-id>:role/<your-oidc-role>`
   - optional `AWS_REGION` (default is `us-east-1` if omitted)
4. **Commit `samconfig.toml`** from your machine (after `sam deploy --guided`) so CI/CD can deploy non-interactively — it should **not** contain API keys (those stay in Secrets Manager per this template). Or hard-code `--stack-name` and `--parameter-overrides` in `deploy.yml`.
5. To deploy on every merge to `main`, edit `deploy.yml` and add a `push:` trigger (see comments in that file).

Example trust policy (replace `<ACCOUNT_ID>`, `<ORG>`, `<REPO>`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": [
            "repo:<ORG>/<REPO>:ref:refs/heads/main",
            "repo:<ORG>/<REPO>:ref:refs/heads/master",
            "repo:<ORG>/<REPO>:pull_request"
          ]
        }
      }
    }
  ]
}
```

## Tests (unit)

These are lightweight unit tests that mock external services (no NewsAPI/Anthropic/Alpaca calls).

Run:

```bash
python3 -m unittest -q tests/test_sentiment_analyzer.py tests/test_trade_executor.py
```

## Backtest Helper Script

`analysis/backtest_report.py` helps with threshold research:

- scans DynamoDB `item_type=SENTIMENT`
- reads `sentiment_score` + `market_price_at_news`
- fetches Alpaca closes at `+15m` and `+60m`
- prints average return by sentiment-score bucket

Run:

```bash
export DYNAMODB_TABLE_NAME=TradingNewsSentiment
export ALPACA_API_KEY=...
export ALPACA_SECRET_KEY=...
python3 analysis/backtest_report.py
```


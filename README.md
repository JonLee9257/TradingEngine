# Serverless Trading System (News -> Sentiment -> Paper Trades)

This project implements the requested pipeline using AWS serverless components and Python 3.12.

Architecture:
1. EventBridge cron triggers `news_fetcher` on weekdays at `09:30` (UTC by default).
2. `news_fetcher` fetches recent news from NewsAPI for the top 10 symbols and sends the payload to an SQS queue.
3. `sentiment_analyzer` consumes the SQS message, calls Claude (Anthropic) to score sentiment, and stores results + backtesting price context in DynamoDB.
4. `sentiment_analyzer` then invokes `trade_executor` synchronously.
5. `trade_executor` reads the sentiment for the given `run_id` from DynamoDB and places paper trades via Alpaca (idempotent per symbol per run).
6. DynamoDB Streams triggers `sentiment_to_s3_parquet`, which appends new `SENTIMENT` rows into an S3 Parquet lake.
7. A weekly EventBridge Scheduler job runs an ECS Fargate VectorBT backtest task to evaluate threshold quality.

## Repo Layout

- `lambdas/news_fetcher/` (NewsAPI -> SQS)
- `lambdas/sentiment_analyzer/` (SQS -> Claude sentiment + historical price context -> DynamoDB -> invoke trade executor)
- `lambdas/trade_executor/` (DynamoDB sentiment + strategy config -> Alpaca paper trades)
- `lambdas/sentiment_to_s3_parquet/` (DynamoDB stream -> S3 Parquet sink via awswrangler)
- `analysis/backtest_report.py` (scan SENTIMENT data + forward returns report)
- `scripts/dynamodb_native_export.py` (enable PITR + start native export to S3)
- `backtester/` (Dockerized VectorBT engine + ECS task definition template)
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

Market-session handling:

- If `publishedAt` is in regular hours (09:30-16:00 ET), price lookup starts at that timestamp.
- If `publishedAt` is outside regular hours, lookup shifts to the next regular open (09:30 ET on next trading weekday).
- `sentiment_analyzer` first tries Alpaca market calendar (`/v2/calendar`) to handle holidays/closures; if unavailable, it falls back to weekday-only next-open logic.

## Strategy-Based Trading

`trade_executor` checks strategy config before placing orders.

Expected strategy item in DynamoDB:

- PK (`run_id`): `STRATEGY#<SYMBOL>`
- SK (`sort_key`): `LATEST`
- Fields:
  - `is_active` (Boolean)
  - `optimized_threshold` (Decimal)

Trading rule:

- Only trade when:
  - `is_active == true`, and
  - `sentiment_score >= optimized_threshold`
- If `TradeOnlyAtClose=true`, trading is additionally allowed only in the final `CloseWindowMinutes` of regular session (default: 5 minutes before close).

If strategy config is missing, defaults are conservative (`is_active=false`, threshold=`1.0`), so no accidental trade.

## How retries & safety work

- SQS queue uses a DLQ with `maxReceiveCount: 3`.
- Each Lambda uses exponential backoff retry logic for its external API calls:
  - NewsAPI (news_fetcher)
  - Claude (sentiment_analyzer)
  - Alpaca (trade_executor)
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

It writes candidate results to DynamoDB as:
- `run_id=BACKTEST#<SYMBOL>`
- `sort_key=<UTC timestamp>`
- `item_type=BACKTEST`

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


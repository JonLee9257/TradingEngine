# Serverless Trading System (News -> Sentiment -> Paper Trades)

This project implements the requested pipeline using AWS serverless components and Python 3.12.

Architecture:
1. EventBridge cron triggers `news_fetcher` on weekdays at `09:30` (UTC by default).
2. `news_fetcher` fetches recent news from NewsAPI for the top 10 symbols and sends the payload to an SQS queue.
3. `sentiment_analyzer` consumes the SQS message, calls Claude (Anthropic) to score sentiment, and stores results in DynamoDB.
4. `sentiment_analyzer` then invokes `trade_executor` synchronously.
5. `trade_executor` reads the sentiment for the given `run_id` from DynamoDB and places paper trades via Alpaca (idempotent per symbol per run).

## Repo Layout

- `lambdas/news_fetcher/` (NewsAPI -> SQS)
- `lambdas/sentiment_analyzer/` (SQS -> Claude sentiment -> DynamoDB -> invoke trade executor)
- `lambdas/trade_executor/` (DynamoDB -> Alpaca paper trades)
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

## How retries & safety work

- SQS queue uses a DLQ with `maxReceiveCount: 3`.
- Each Lambda uses exponential backoff retry logic for its external API calls:
  - NewsAPI (news_fetcher)
  - Claude (sentiment_analyzer)
  - Alpaca (trade_executor)
- DynamoDB writes are idempotent per `run_id` and symbol:
  - Sentiment items use conditional writes (`attribute_not_exists`) to avoid duplicates.
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


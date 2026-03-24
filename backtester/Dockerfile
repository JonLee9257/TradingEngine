FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    NUMBA_CACHE_DIR=/tmp/numba_cache

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
      "vectorbt[full]" \
      pandas \
      boto3 \
      pyarrow \
      awswrangler \
      alpaca-py \
      pydantic

COPY strategies /app/strategies
COPY models /app/models
COPY backtester/backtest_engine.py /app/backtest_engine.py
COPY backtester/strategy_promotion_runner.py /app/strategy_promotion_runner.py

CMD ["python", "/app/backtest_engine.py"]

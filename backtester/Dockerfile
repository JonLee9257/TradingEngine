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
      awswrangler

COPY backtester/backtest_engine.py /app/backtest_engine.py

CMD ["python", "/app/backtest_engine.py"]

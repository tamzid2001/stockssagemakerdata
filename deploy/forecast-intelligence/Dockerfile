FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements-forecast-intelligence.txt requirements-forecast-intelligence-service.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements-forecast-intelligence-service.txt \
    && pip install "chronos-forecasting>=2.0,<3"

COPY forecast_intelligence ./forecast_intelligence
COPY config ./config

RUN useradd --create-home --uid 10001 quantura \
    && chown -R quantura:quantura /app
USER quantura

ENV PORT=8080
EXPOSE 8080

CMD ["sh", "-c", "uvicorn forecast_intelligence.service:app --host 0.0.0.0 --port ${PORT} --workers 1"]

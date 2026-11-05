FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY app/ ./app/

# Run as a non-root system user — required by most hardened container runtimes
# and Kubernetes PodSecurityAdmission policies.
RUN useradd --no-create-home --system appuser
USER appuser

ENV PYTHONUNBUFFERED=1
ENV LOG_FORMAT=json
ENV LOG_LEVEL=INFO

EXPOSE 8000

# Use /health/live (process-only liveness) not /health (readiness + dependency pings)
# so a temporary MongoDB or Redis hiccup doesn't cause the container to restart.
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health/live')" || exit 1

# Single worker: the Kafka consumer runs on asyncio and must not be forked
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
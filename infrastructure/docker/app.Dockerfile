# FraudStream application image: FastAPI API, Kafka persistence consumer, one-shot app-init
# (migrations + login accounts) and the optional python-kafka fallback processor.
# Model artifacts are NOT baked in: ml/artifacts is mounted at runtime (read-only by default).
FROM python:3.12-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update \
 && apt-get install -y --no-install-recommends tini libgomp1 fonts-dejavu-core fonts-droid-fallback fonts-nanum \
 && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

RUN useradd -m -u 10001 app
WORKDIR /app
COPY backend/ /app/backend/
COPY ml/ /app/ml/
COPY simulator/ /app/simulator/
COPY streaming/ /app/streaming/
COPY benchmarks/ /app/benchmarks/
COPY infrastructure/docker/healthcheck.py infrastructure/docker/app-init.sh /app/infrastructure/docker/
# Volume mount points must exist (owned by the app user) in the image: Docker creates missing
# mount points as root, and seeds an EMPTY named volume with the image directory's ownership.
RUN mkdir -p /app/ml/artifacts /data/db /data/uploads /state \
 && chown -R app:app /data /state \
 && chmod 0755 /app/infrastructure/docker/app-init.sh

USER app
ENV PYTHONPATH=/app:/app/backend FRAUDSTREAM_DATA_DIR=/data RUNTIME_CONFIG_DIR=/state
WORKDIR /app/backend
EXPOSE 8000
ENTRYPOINT ["tini", "--"]
# One worker: the API keeps process-local runtime state (simulation manager, model cache, SSE broker).
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", \
     "--proxy-headers", "--forwarded-allow-ips", "*", "--timeout-graceful-shutdown", "10"]

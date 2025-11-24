# Following the official Home Assistant add-on pattern
ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:latest
FROM $BUILD_FROM

ENV PATH="/opt/venv/bin:${PATH}"

# Install Python + FastAPI dependencies inside a venv to avoid PEP 668 errors
RUN apk add --no-cache python3 py3-pip \
    && python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir fastapi==0.115.5 uvicorn[standard]==0.30.6 httpx==0.27.2

# Copy root filesystem and app
COPY rootfs /

WORKDIR /usr/src/app

RUN chmod a+x /etc/services.d/charles_api/run \
    && chmod a+x /etc/cont-init.d/00-banner.sh

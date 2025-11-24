# Following the official Home Assistant add-on pattern
ARG BUILD_FROM
FROM $BUILD_FROM

# Install Python + FastAPI dependencies
RUN apk add --no-cache python3 py3-pip \
    && pip3 install --no-cache-dir fastapi==0.115.5 uvicorn[standard]==0.30.6 httpx==0.27.2

# Copy root filesystem and app
COPY rootfs /

WORKDIR /usr/src/app

RUN chmod a+x /etc/services.d/charles_api/run \
    && chmod a+x /etc/cont-init.d/00-banner.sh

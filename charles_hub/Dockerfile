# Following the official Home Assistant add-on pattern
ARG BUILD_FROM
FROM $BUILD_FROM

# Copy root filesystem
COPY rootfs /

RUN chmod a+x /etc/services.d/charles_hub/run /etc/services.d/charles_hub/finish

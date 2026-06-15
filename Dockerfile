# Project:   dfe-archiver
# File:      Dockerfile
# Purpose:   Runtime container for dfe-archiver (rustlib container contract)
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Runtime dependencies resolved from hyperi-rustlib features:
#   transport-kafka  -> librdkafka1 (Confluent repo, dynamic linking)
#   spool, tiered-sink -> libzstd1
#   (transitive)     -> libssl3, zlib1g

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl netcat-openbsd iputils-ping gnupg \
    && curl -fsSL https://packages.confluent.io/clients/deb/archive.key \
       | gpg --dearmor -o /usr/share/keyrings/confluent-clients.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/confluent-clients.gpg] \
       https://packages.confluent.io/clients/deb noble main" \
       > /etc/apt/sources.list.d/confluent-clients.list \
    && apt-get update && apt-get install -y --no-install-recommends \
       librdkafka1 libssl3 libzstd1 zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY dfe-archiver /usr/local/bin/dfe-archiver
RUN chmod +x /usr/local/bin/dfe-archiver

RUN userdel -r ubuntu 2>/dev/null || true \
    && useradd --create-home --uid 1000 appuser
USER appuser

EXPOSE 9090

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:9090/healthz > /dev/null || exit 1

ENTRYPOINT ["dfe-archiver"]
CMD ["--config", "/etc/dfe/archiver.yaml"]

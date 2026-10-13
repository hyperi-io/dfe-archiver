#!/usr/bin/env bash
# Project:   dfe-archiver
# File:      scripts/pull-test-images.sh
# Purpose:   Pull every container image the e2e fixtures start, before any test runs
# Language:  Bash
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# nextest runs this as the `pull-test-images` setup script (.config/nextest.toml),
# so no test spends its own timeout on a pull. The fixtures in
# crates/archiver/tests/common/mod.rs read their pins from the assignments below.

set -euo pipefail

# Each tag sits on the line after its `renovate:` comment, where the shared Renovate regex reads it.

# Pinned rather than `latest`: a floating tag retargets the suite with nothing in the diff to explain it.
# renovate: datasource=docker depName=mcr.microsoft.com/azure-storage/azurite
AZURITE_TAG="3.36.0"

# The maintained GCS emulator, pinned for the same reason as Azurite.
# renovate: datasource=docker depName=fsouza/fake-gcs-server
FAKE_GCS_TAG="1.55.1"

# The JVM image, the same pin scalo's own tests use: apache/kafka-native before 4.4.0 segfaults in getpwuid on ~2% of starts.
# renovate: datasource=docker depName=apache/kafka
KAFKA_TAG="4.3.1"
# The digest of KAFKA_TAG, on its own line because the Renovate regex stops at a colon.
KAFKA_DIGEST="sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837"

# The semver line only, the same pin as dfe-fetcher: the CalVer tags (2026.07.0 on) exit 55 with "License activation failed" unless LOCALSTACK_AUTH_TOKEN is set, so reject a CalVer bump.
# 4.x is the newest line that boots with no token, verified on 4.14.0 (build 2026-02-26).
# renovate: datasource=docker depName=localstack/localstack versioning=semver
LOCALSTACK_TAG="4.14"

IMAGES=(
    "mcr.microsoft.com/azure-storage/azurite:${AZURITE_TAG}"
    "fsouza/fake-gcs-server:${FAKE_GCS_TAG}"
    "apache/kafka:${KAFKA_TAG}@${KAFKA_DIGEST}"
    "localstack/localstack:${LOCALSTACK_TAG}"
)

main() {
    # With no Docker daemon the fixtures skip, or fail the run in CI, and say why.
    if ! docker version --format '{{.Server.Version}}' >/dev/null 2>&1; then
        echo "pull-test-images: no Docker daemon, nothing pulled" >&2
        return 0
    fi
    local image
    for image in "${IMAGES[@]}"; do
        if docker image inspect "${image}" >/dev/null 2>&1; then
            continue
        fi
        echo "pull-test-images: pulling ${image}" >&2
        docker pull --quiet "${image}" >/dev/null
    done
}

main "$@"

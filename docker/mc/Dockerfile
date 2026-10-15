# See docker/iceberg/minio/Dockerfile's header: quay.io/minio/mc and
# minio/mc (Docker Hub) are both unpullable now that MinIO has discontinued
# prebuilt community images. This builds the mc client from MinIO's last
# published binary release, fetched from GitHub Releases and checksum-verified.
FROM debian:bookworm-slim

ARG MC_RELEASE=RELEASE.2025-08-13T08-35-41Z
ARG MC_SHA256=01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /usr/bin/mc \
      "https://github.com/minio/mc/releases/download/${MC_RELEASE}/mc.linux-amd64.${MC_RELEASE}" \
    && echo "${MC_SHA256}  /usr/bin/mc" | sha256sum -c - \
    && chmod +x /usr/bin/mc

ENTRYPOINT ["mc"]

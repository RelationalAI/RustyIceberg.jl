# MinIO discontinued prebuilt community images (see minio/minio's README as
# of Oct 2025: "THIS REPOSITORY IS NO LONGER MAINTAINED" / "distributed as
# source code only") -- quay.io/minio/minio and minio/minio (Docker Hub) are
# both unpullable now, and even upstream's own Dockerfile is broken (it FROMs
# minio/minio:latest, the very image that no longer exists).
#
# This builds a drop-in replacement from MinIO's last published binary
# release instead, fetched directly from GitHub Releases (still served,
# unlike dl.min.io which now returns 410) and checksum-verified.
FROM debian:bookworm-slim

ARG MINIO_RELEASE=RELEASE.2025-09-07T16-13-09Z
ARG MINIO_SHA256=7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /usr/bin/minio \
      "https://github.com/minio/minio/releases/download/${MINIO_RELEASE}/minio.linux-amd64.${MINIO_RELEASE}" \
    && echo "${MINIO_SHA256}  /usr/bin/minio" | sha256sum -c - \
    && chmod +x /usr/bin/minio

EXPOSE 9000 9001
VOLUME ["/data"]
ENTRYPOINT ["minio"]
CMD ["server", "/data"]

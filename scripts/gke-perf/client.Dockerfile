# syntax=docker/dockerfile:1
# Client image of the GKE durable performance matrix
# (.github/workflows/gke-perf.yml, k8s/gke-perf-job). Internal test image,
# pushed only as ghcr.io/faberline/lumen-perf-client:sha-<git12>.
#
# It holds the `it` test binary of this commit, built with the repo's
# [profile.release] and the same toolchain as the server image (Dockerfile),
# plus kubectl for attach mode and scripts/gke-perf/cell.sh. No
# CARGO_PROFILE_* override: gke-perf.yml refuses them before the build.
#
#   docker buildx build -f scripts/gke-perf/client.Dockerfile .
FROM rust:1.92-slim-bookworm AS builder
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
# Same release gke-test.yml stages for the test-client image, checked by digest.
RUN curl -fsSL https://dl.k8s.io/release/v1.37.0/bin/linux/amd64/kubectl -o /usr/local/bin/kubectl \
 && echo '6129359f4e1f3848a5572ccb0b26cf28b8ca08cef38c95a765b2f64a2c961a2f  /usr/local/bin/kubectl' | sha256sum -c - \
 && chmod 0755 /usr/local/bin/kubectl
COPY . .
# target/ is a cache mount, so copy the binary out in the same RUN. Cargo
# names the test binary it-<hash>; take the one path it reports.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo test --release --locked -p lumen --test it --no-run 2>&1 | tee /tmp/build.log \
 && exe="$(sed -n 's|^ *Executable tests/it/main.rs (\(target/release/deps/it-[0-9a-f]*\))$|\1|p' /tmp/build.log)" \
 && [ "$(printf '%s\n' "$exe" | grep -c .)" -eq 1 ] \
 && cp "$exe" /usr/local/bin/lumen-perf-it

FROM debian:bookworm-slim
LABEL org.opencontainers.image.source=https://github.com/faberline/lumen
LABEL org.opencontainers.image.description="Lumen durable performance client (internal test image)"
COPY --from=builder /usr/local/bin/kubectl /usr/local/bin/kubectl
COPY --from=builder /usr/local/bin/lumen-perf-it /usr/local/bin/lumen-perf-it
COPY --chmod=0755 scripts/gke-perf/cell.sh /usr/local/bin/lumen-perf-cell
USER 65532:65532
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/lumen-perf-cell"]

# -----------------------
# Builder
# -----------------------
FROM rust:alpine3.23 AS builder

ARG TARGETPLATFORM
ARG BUILDPLATFORM

ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

# musl-dev and build-base are needed for the static link. openssl is here for
# the protocol clients: unlike a pure filesystem tool, misorder dials TLS
# endpoints when a scenario points at a real vendor sandbox.
RUN apk add --no-cache \
    musl-dev \
    build-base \
    pkgconfig \
    openssl-dev \
    openssl-libs-static \
    git

WORKDIR /src

COPY .cargo/ .cargo/
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY apps/ apps/

RUN cargo build --release --package misorder-cli --bin mis

# -----------------------
# Runtime
# -----------------------
FROM alpine:3.23 AS runner

# docker-cli is not installed on purpose. misorder talks to the daemon's HTTP
# API directly through the mounted socket, so shelling out to a client binary
# would be a second, slower path to the same place.
#
# ca-certificates is for TLS to vendor sandboxes. procps is for looking at a
# stuck process without rebuilding the image.
RUN apk add --no-cache \
    ca-certificates \
    procps

COPY --from=builder /src/target/release/mis /usr/local/bin/mis

# Where a bind-mounted scenario is expected.
WORKDIR /work

# SIGNALS, and this matters more than it looks.
#
# An ENTRYPOINT process runs as PID 1, and the kernel installs no default signal
# handlers for PID 1. A process that does not explicitly handle SIGTERM ignores
# it, so `docker stop` waits out its timeout and then SIGKILLs.
#
# For misorder that is worse than a slow shutdown: SIGKILL cannot be caught, so
# the dependency containers this process started are never torn down. They then
# hold the ports the next run needs, and the failure surfaces as the *next*
# run failing to start, which is the hardest kind of bug to trace back.
#
# Run with `--init`, and mount the daemon socket:
#
#   docker run --rm --init \
#     -v /var/run/docker.sock:/var/run/docker.sock \
#     -v "$PWD:/work" \
#     ghcr.io/anthid-labs/misorder run scenario.toml --seed 8837291
ENTRYPOINT ["/usr/local/bin/mis"]

# No CMD on purpose: there is no safe default subcommand. `run` and `fuzz` both
# start containers the moment this one starts, so the operator states which.

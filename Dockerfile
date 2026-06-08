# syntax=docker/dockerfile:1.7-labs
# Our Dockerfile relies on the PATHFINDER_FORCE_VERSION build-time variable being set.
# This is required so that we don't have to copy the .git directory into the layer which
# might cause caches to be invalidated even if that's unnecessary.

########################################
# Stage 1: Build the pathfinder binary #
########################################
# Note that we're explicitly using the Debian bookworm image to make sure we're
# compatible with the Debian container we'll be copying the pathfinder
# executable to.
#
# IMPORTANT: keep the Rust version in this image tag equal to the channel in
# rust-toolchain.toml. The COPY steps below exclude rust-toolchain.toml from the
# build context, so rustup never sees the pin and the build always uses the Rust
# that ships in this image (we exclude it so rustup doesn't re-download the
# toolchain's components on every build)
FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:0.1.77-rust-1.95.0-slim-bookworm AS cargo-chef
WORKDIR /usr/src/pathfinder

FROM --platform=$BUILDPLATFORM cargo-chef AS rust-planner
COPY --exclude=rust-toolchain.toml . .
# carg-chef prepare examines your project and builds a recipe that captures
# the set of information required to build your dependencies.
RUN cargo chef prepare --recipe-path recipe.json

FROM --platform=$BUILDPLATFORM cargo-chef AS rust-builder
ARG CARGO_EXTRA_ARGS
ARG TARGETARCH
COPY ./build/prepare.sh prepare.sh
RUN TARGETARCH=${TARGETARCH} ./prepare.sh

# The recipe.json is the equivalent of the Python requirements.txt file - it is the only
# input required for cargo chef cook, the command that will build out our dependencies.
COPY --from=rust-planner /usr/src/pathfinder/recipe.json recipe.json
COPY ./build/cargo-chef-cook.sh ./cargo-chef-cook.sh
RUN TARGETARCH=${TARGETARCH} ./cargo-chef-cook.sh --profile release-lto --recipe-path recipe.json --package pathfinder --bin pathfinder ${CARGO_EXTRA_ARGS}

# Compile the actual libraries and binary now
COPY --exclude=rust-toolchain.toml . .
ARG PATHFINDER_FORCE_VERSION
COPY ./build/cargo-build.sh ./cargo-build.sh
RUN TARGETARCH=${TARGETARCH} \
    PATHFINDER_FORCE_VERSION=${PATHFINDER_FORCE_VERSION} \
    ./cargo-build.sh --locked --profile release-lto --package pathfinder --bin pathfinder ${CARGO_EXTRA_ARGS} \
    && cp target/*-unknown-linux-gnu/release-lto/pathfinder pathfinder-${TARGETARCH}

#######################
# Final Stage: Runner #
#######################
# Note that we're explicitly using the Debian bookworm image to make sure we're
# compatible with the Rust builder we've built the pathfinder executable in.
FROM debian:bookworm-slim AS runner
ARG TARGETARCH

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates tini binutils && rm -rf /var/lib/apt/lists/*
RUN groupadd --gid 1000 pathfinder && useradd --no-log-init --uid 1000 --gid pathfinder --no-create-home pathfinder

COPY --from=rust-builder /usr/src/pathfinder/pathfinder-${TARGETARCH} /usr/local/bin/pathfinder

# Hack to enable `ld` link with glibc without the libc6-dev package being installed
RUN if [ "${TARGETARCH}" = "amd64" ]; then ln -s /lib/x86_64-linux-gnu/libc.so.6 /lib/x86_64-linux-gnu/libc.so; fi

# Create directory and volume for persistent data
RUN install --owner 1000 --group 1000 --mode 0755 -d /usr/share/pathfinder/data
VOLUME /usr/share/pathfinder/data

USER 1000:1000
EXPOSE 9545
WORKDIR /usr/share/pathfinder/data

# this is required to have exposing ports work from docker, the default is not this.
ENV PATHFINDER_HTTP_RPC_ADDRESS="[::]:9545"

# this has been changed in #335 to follow docker best practices example; every
# time it is changed it will be a breaking change. this allows `docker run
# eqlabs/pathfinder --help` to give an introductory path to configuration.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/pathfinder"]

# empty CMD is needed and cannot be --help because otherwise configuring from
# environment variables only would be impossible and require a workaround.
CMD []

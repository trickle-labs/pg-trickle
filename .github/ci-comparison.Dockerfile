ARG BUILDER_IMAGE
FROM docker:cli AS docker-client
FROM ${BUILDER_IMAGE}

COPY --from=docker-client /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker-client /usr/local/libexec/docker/cli-plugins/ /usr/local/libexec/docker/cli-plugins/
RUN apt-get update && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*
RUN rustup toolchain install 1.99.1 --profile minimal && rustup default 1.99.1
RUN cargo install --locked cargo-nextest --version 0.9.148

# =============================================================================
# Devcontainer Dockerfile for pg_trickle development
#
# Based on postgres:18.6 to get matching PG headers + pg_config, then layers
# Rust toolchain, cargo-pgrx, and development tools on top.
# =============================================================================
FROM postgres:18.6

ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=${USER_UID}

# ── System dependencies ─────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Build essentials
    build-essential \
    pkg-config \
    ca-certificates \
    curl \
    git \
    gnupg \
    # PostgreSQL dev headers (pgrx bindgen)
    postgresql-server-dev-18 \
    libreadline-dev \
    zlib1g-dev \
    libssl-dev \
    libicu-dev \
    bison \
    flex \
    # clang/llvm (pgrx bindgen requirement)
    libclang-dev \
    clang \
    # Useful dev tools
    sudo \
    locales \
    less \
    procps \
    lsb-release \
    && rm -rf /var/lib/apt/lists/*

# ── Locale ───────────────────────────────────────────────────────────────────
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

# ── Non-root user ────────────────────────────────────────────────────────────
RUN groupadd --gid ${USER_GID} ${USERNAME} \
    && useradd --uid ${USER_UID} --gid ${USER_GID} -m ${USERNAME} -s /bin/bash \
    && echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/${USERNAME} \
    && chmod 0440 /etc/sudoers.d/${USERNAME}

# ── Rust toolchain (installed for vscode user) ──────────────────────────────
USER ${USERNAME}
ENV HOME=/home/${USERNAME}

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain stable --component clippy,rustfmt
ENV PATH="${HOME}/.cargo/bin:${PATH}"

# ── cargo-pgrx ──────────────────────────────────────────────────────────────
# Keep `cargo pgrx init` out of this Dockerfile: it writes user/runtime state
# under /home/vscode/.pgrx, which is volume-mounted in the devcontainer.
# Initialization is performed in .devcontainer/devcontainer.json lifecycle hooks
# after mounts are attached and ownership is corrected.
RUN cargo install --locked cargo-pgrx --version 0.18.0

# ── just (command runner) ───────────────────────────────────────────────────
RUN cargo install just

# ── Ensure pg_config is on PATH ─────────────────────────────────────────────
ENV PATH="/usr/lib/postgresql/18/bin:${PATH}"

USER root

# ── Default working directory ────────────────────────────────────────────────
WORKDIR /workspaces/pg-stream

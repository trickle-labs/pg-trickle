# The R53 workload's compiler and PostgreSQL runtime remain pinned.
ARG S1_BUILDER=ghcr.io/trickle-labs/pg-trickle/builder@sha256:8d0446c21ab3273b55c045a39c49120e9d7cde8e970954c3f81e7bee194fad95
FROM ${S1_BUILDER} AS compiler
ARG TARGETARCH
RUN rustup toolchain install 1.98.0 --profile minimal && rustup default 1.98.0

FROM compiler AS trickle
ARG TRICKLE_SOURCE=.
WORKDIR /build
COPY ${TRICKLE_SOURCE}/Cargo.toml ${TRICKLE_SOURCE}/Cargo.lock ${TRICKLE_SOURCE}/build.rs ${TRICKLE_SOURCE}/pg_trickle.control ./
COPY ${TRICKLE_SOURCE}/src/ src/
COPY ${TRICKLE_SOURCE}/verification/ verification/
COPY ${TRICKLE_SOURCE}/benches/ benches/
COPY ${TRICKLE_SOURCE}/sql/ sql/
COPY ${TRICKLE_SOURCE}/scripts/gen_test_schema.py scripts/gen_test_schema.py
COPY ${TRICKLE_SOURCE}/.cargo/config.toml .cargo/config.toml
# Old checkout mtimes must not reuse another source tree's compiled library.
RUN --mount=type=cache,id=s1-${TARGETARCH}-registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=s1-${TARGETARCH}-git,sharing=locked,target=/usr/local/cargo/git \
    --mount=type=cache,id=s1-${TARGETARCH}-trickle,target=/build/target \
    sha256sum Cargo.lock > /tmp/lock.sha256 && \
    cargo clean -p pg_trickle --release && \
    CARGO_BUILD_JOBS=2 cargo pgrx package --pg-config /usr/bin/pg_config --out-dir /package && \
    sha256sum -c /tmp/lock.sha256

FROM compiler AS mdm
WORKDIR /build
COPY _s1/mdm/ ./
COPY benchmarks/s1/mdm-grants.patch /tmp/mdm-grants.patch
RUN git apply /tmp/mdm-grants.patch
RUN --mount=type=cache,id=s1-${TARGETARCH}-registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=s1-${TARGETARCH}-mdm,target=/build/target \
    sha256sum Cargo.lock > /tmp/lock.sha256 && \
    CARGO_BUILD_JOBS=2 cargo pgrx package --pg-config /usr/bin/pg_config --out-dir /package && \
    sha256sum -c /tmp/lock.sha256

FROM compiler AS react
WORKDIR /build
COPY _s1/react/Cargo.toml _s1/react/Cargo.lock ./
COPY _s1/react/src/ src/
RUN --mount=type=cache,id=s1-${TARGETARCH}-registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=s1-${TARGETARCH}-react,target=/build/target \
    cargo build --locked --release --jobs 2 --no-default-features --features pg18 && \
    cp target/release/libpg_react.so /libpg_react.so

FROM postgres:18.4-bookworm@sha256:efef99e1558f86089bc84bece29208c0777a185ff717ec7fa288a652ce2d0adf
COPY --from=trickle /package/usr/ /usr/
COPY --from=mdm /package/usr/ /usr/
COPY --from=react /libpg_react.so /usr/lib/postgresql/18/lib/pg_react.so
COPY _s1/react/pg_react.control _s1/react/sql/ /usr/share/postgresql/18/extension/
COPY _s1/mdm/sql/archive/pg_mdm--0.1.0.sql _s1/mdm/sql/archive/pg_mdm--0.2.0.sql /usr/share/postgresql/18/extension/
COPY _s1/mdm/tests/e2e.sql _s1/mdm/tests/e2e_policy.sql /tests/
COPY _s1/mdm/sql/configure_helper.sql /sql/configure_helper.sql
COPY _s1/react/bin/pg-reactd /usr/local/bin/pg-reactd
COPY _s1/react/docker/00-pg-trickle.sql /docker-entrypoint-initdb.d/10-pg-react.sql
# Reconstruct the R53 stack's exact patch-version admission; keep every runtime/capability check.
RUN sed -i 's/0\.108\.0/0.108.3/g' \
    /usr/share/postgresql/18/extension/pg_react--0.46.0.sql \
    /usr/share/postgresql/18/extension/pg_react--0.46.1.sql
ENV PG_REACT_INIT_VERSION=0.46.1
LABEL pg_mdm.source_revision="eef7937180d504e62ce1076074bbeed8cef5d5a7"
LABEL qualification.react_pg_trickle_pin="0.108.3"
LABEL qualification.mdm_grant_patch="sha256:a3dd34646b51a89cb52b3e424dfe17c48dab0f632ef585cb06cfbfa4934de388"
USER postgres

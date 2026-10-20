FROM pg-trickle-1168:arm64-sdk@sha256:de453c245823bdb68c341e5b648a08f008e7cf67a336615a753baadd8c1f9150 AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock pg_trickle.control ./
COPY src/ src/
COPY verification/ verification/
COPY benches/ benches/
ENV CARGO_NET_OFFLINE=true
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/usr/local/cargo/git --mount=type=cache,target=/build/target cargo pgrx package --pg-config /usr/bin/pg_config && mkdir /artifact && cp target/release/pg_trickle-pg18/usr/lib/postgresql/18/lib/pg_trickle.so /artifact/
FROM pgreact:issue11-compatibility@sha256:3cdd0ae0ac2b15f42cabdfc08c41b758360807148f6a5399f05b9bff4066453a
COPY --from=builder /artifact/pg_trickle.so /usr/lib/postgresql/18/lib/pg_trickle.so
LABEL pg_trickle.react11_candidate="pinned d8bb8eb5 plus USING correction" \
      pg_trickle.base_source_archive_sha256="b8d4c59ca3770c305659255e09adde303b4d220228392c415ae363aa7be0a2da" \
      pg_trickle.source_archive_sha256="47558b48443c78336c0e00994f2e31b33b995382418fc1d5ceb79dab955e89de" \
      pg_trickle.correction_patch_sha256="1ef4bd312ea71860076f61f865a107dd4a2ee4c6dd9e43bdaf9919a318c8056f"

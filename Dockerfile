FROM rust:1.88-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests

RUN cargo build --release

FROM debian:bookworm-slim

COPY --from=builder /app/target/release/minio-rust /usr/bin/minio
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY dockerscripts/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh

RUN chmod +x /usr/bin/minio /usr/bin/docker-entrypoint.sh

ENV MINIO_ROOT_USER_FILE=access_key \
    MINIO_ROOT_PASSWORD_FILE=secret_key \
    MINIO_ACCESS_KEY_FILE=access_key \
    MINIO_SECRET_KEY_FILE=secret_key \
    MINIO_CONFIG_ENV_FILE=config.env

EXPOSE 9000
EXPOSE 9001

VOLUME ["/data"]

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]
CMD ["minio"]

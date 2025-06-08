FROM rust:1.77-slim AS builder
WORKDIR /app
COPY Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release || true
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/mock_overflow_server /usr/local/bin/
EXPOSE 8080
ENTRYPOINT ["mock_overflow_server"]
CMD ["--bind", "0.0.0.0:8080", "--concurrency-limit", "200", "--queue-capacity", "1000", "--processing-delay-ms", "30"]

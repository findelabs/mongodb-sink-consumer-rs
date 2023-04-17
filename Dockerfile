from rust:slim as builder

RUN mkdir /app 
RUN mkdir /app/bin 

COPY src /app/src/
COPY Cargo.toml /app

RUN apt-get update && apt-get install -y libssl-dev pkg-config cmake cyrus-sasl-devel
RUN cargo install --path /app --root /app
RUN strip app/bin/mongodb-sink-consumer-rs

FROM debian:bullseye-slim
WORKDIR /app
COPY --from=builder /app/bin/ ./

ENTRYPOINT ["/app/mongodb-sink-consumer-rs"]

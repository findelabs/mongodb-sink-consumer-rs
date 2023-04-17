from rust:slim as builder

ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

RUN mkdir /app 
RUN mkdir /app/bin 

COPY src /app/src/
COPY Cargo.toml /app

RUN apt-get update && apt-get install -y libssl-dev pkg-config cmake libsasl2-modules-gssapi-mit libsasl2-dev build-essential
RUN cargo install --path /app --root /app
RUN strip app/bin/mongodb-sink-consumer-rs

FROM debian:bullseye-slim
WORKDIR /app
COPY --from=builder /app/bin/ ./

ENTRYPOINT ["/app/mongodb-sink-consumer-rs"]

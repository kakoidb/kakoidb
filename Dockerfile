#####################
# Build environment #
#####################
FROM rust:1.34.2-slim AS builder

RUN apt update && apt install -y libclang-dev clang

RUN USER=root cargo new --bin /app
WORKDIR /app

ADD ./Cargo.lock /app/Cargo.lock
ADD ./Cargo.toml /app/Cargo.toml

# Build and cache dependencies
RUN cargo build --release
RUN rm src/*.rs

ADD ./src /app/src

RUN rm /app/target/release/deps/kakoidb*
RUN cargo build --release

#######################
# Runtime environment #
#######################
FROM debian:stretch-slim

RUN apt-get update && apt-get install -y libgcc1 libstdc++6 && apt-get clean

ADD DockerSettings.toml ./Settings.toml
VOLUME /storage

COPY --from=builder \
  /app/target/release/kakoidb \
  /usr/local/bin/

CMD ["/usr/local/bin/kakoidb"]

FROM rust:1.62-slim-bullseye

# Install the required platform dependencies
RUN apt-get update && \
    apt-get install -y libssl-dev libudev-dev pkg-config zlib1g-dev llvm clang cmake make libprotobuf-dev protobuf-compiler bash curl screen

RUN rustup component add rustfmt

CMD /bin/bash

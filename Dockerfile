# Multi-stage build: compile live_feed with the live adapter, run a minimal runtime image.
# Build:  docker build -t order-book-live .
# Run:    docker run --rm -p 8080:8080 -e PORT=8080 order-book-live

FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    cmake \
    g++ \
    git \
    libssl-dev \
    ninja-build \
    zlib1g-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cmake -S . -B build -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DFH_BUILD_LIVE=ON \
      -DFH_BUILD_TESTS=OFF \
      -DFH_BUILD_BENCH=OFF \
  && cmake --build build --target live_feed

FROM ubuntu:24.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libssl3t64 \
    zlib1g \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /src/build/live_feed /app/live_feed

ENV PORT=8080
ENV SYMBOL=BTCUSDT
ENV MARKET=both

EXPOSE 8080

# PORT env auto-starts the dashboard on 0.0.0.0 (see live_feed apply_env_overrides).
CMD ["/app/live_feed"]

# First validation target: PostgreSQL 18.6 / Debian 12 / linux/arm64.
FROM postgres:18.6-bookworm@sha256:3725f4e2499eef5134592b3b4ab79a543ed7f8e533b05b5b637af926630f6650
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       gcc=4:12.2.0-3 gcc-12=12.2.0-14+deb12u1 make=4.3-4.1 \
       clang-19=1:19.1.7-3~deb12u1 llvm-19=1:19.1.7-3~deb12u1 \
       postgresql-server-dev-18=18.6-1.pgdg12+2 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY . .
RUN make PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config CC=gcc \
    && make PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config install \
    && chown -R postgres:postgres /workspace

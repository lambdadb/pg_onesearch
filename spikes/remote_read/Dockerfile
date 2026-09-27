FROM pg_onesearch:client-contract
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends libcurl4-openssl-dev=7.88.1-10+deb12u15 \
    && rm -rf /var/lib/apt/lists/*
RUN make -C spikes/remote_read PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config CC=gcc \
    && make -C spikes/remote_read PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config install

FROM pg_onesearch:remote-read
COPY spikes/executor /workspace/spikes/executor
RUN make -C spikes/executor PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config CC=gcc \
    && make -C spikes/executor PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config install

FROM pg_onesearch:snapshot-read
COPY spikes/snapshot_executor /workspace/spikes/snapshot_executor
RUN make -C spikes/snapshot_executor PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config CC=gcc \
    && make -C spikes/snapshot_executor PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config install

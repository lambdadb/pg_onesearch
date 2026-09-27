FROM pg_onesearch:snapshot-executor
COPY spikes/commit_worker /workspace/spikes/commit_worker
COPY spikes/snapshot_read /workspace/spikes/snapshot_read
COPY spikes/publication_commit /workspace/spikes/publication_commit
RUN make -C spikes/commit_worker PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config CC=gcc \
    && make -C spikes/commit_worker PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config install \
    && make -C spikes/snapshot_read PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config CC=gcc \
    && make -C spikes/snapshot_read PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config install

FROM pg_onesearch:remote-read
COPY spikes/index_lifecycle /workspace/spikes/index_lifecycle
COPY spikes/change_capture /workspace/spikes/change_capture
COPY spikes/batch_replay /workspace/spikes/batch_replay
COPY spikes/live_compatibility /workspace/spikes/live_compatibility
COPY spikes/snapshot_read /workspace/spikes/snapshot_read
RUN make -C spikes/index_lifecycle PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config CC=gcc \
    && make -C spikes/index_lifecycle PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config install \
    && make -C spikes/snapshot_read PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config CC=gcc \
    && make -C spikes/snapshot_read PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config install

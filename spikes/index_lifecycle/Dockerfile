FROM pg_onesearch:client-contract
COPY spikes/index_lifecycle /workspace/spikes/index_lifecycle
RUN make -C spikes/index_lifecycle PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config CC=gcc \
    && make -C spikes/index_lifecycle PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config install

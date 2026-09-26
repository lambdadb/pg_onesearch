# Built from this checkout by scripts/test-client-contract.sh.
FROM pg_onesearch:client-contract-base
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       python3-venv=3.11.2-1+b1 python3.11-venv=3.11.2-6+deb12u8 \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/client-venv
RUN /opt/client-venv/bin/pip install --no-cache-dir --only-binary=:all: \
    --require-hashes -r spikes/client_contract/requirements.txt
ENV PSYCOPG_IMPL=python

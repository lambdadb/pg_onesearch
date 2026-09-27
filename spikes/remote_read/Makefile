# Isolated candidate: not installed by the product Makefile or evaluation bundle.
MODULE_big = pg_onesearch_remote_probe
OBJS = http.o probe.o
SHLIB_LINK += -lcurl
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

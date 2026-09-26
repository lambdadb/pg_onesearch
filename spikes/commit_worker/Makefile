# Test-only module. Deliberately absent from the product Makefile/package.
MODULE_big = pg_onesearch_commit_probe
OBJS = probe.o
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

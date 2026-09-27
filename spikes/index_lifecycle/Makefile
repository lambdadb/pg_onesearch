# Test-only IAM lifecycle module; excluded from the product bundle.
MODULE_big = pg_onesearch_lifecycle_probe
OBJS = lifecycle.o
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

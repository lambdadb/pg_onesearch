# Isolated planner/executor experiment, not the product extension.
MODULE_big = pg_onesearch_snapshot_executor
OBJS = executor.o
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

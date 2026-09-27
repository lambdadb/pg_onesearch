# Isolated planner/executor experiment, not the product extension.
MODULE_big = pg_onesearch_executor_probe
OBJS = executor.o
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

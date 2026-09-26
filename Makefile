EXTENSION = pg_onesearch
MODULE_big = pg_onesearch
OBJS = src/vector.o
DATA = sql/pg_onesearch--0.1.0-dev.sql
REGRESS = vector
REGRESS_OPTS = --inputdir=test
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
SHLIB_LINK += -lm
include $(PGXS)

# Thin wrapper so `make bench` works for people who expect make.
# All real logic lives in run.sh; run `./run.sh help` for the full command list.

.PHONY: all smoke up down clean status data bench duckdb polars spark report help

all:        ; ./run.sh all
smoke:      ; ./run.sh smoke
up:         ; ./run.sh up
down:       ; ./run.sh down
clean:      ; ./run.sh clean
status:     ; ./run.sh status
data:       ; ./run.sh data
bench:      ; ./run.sh bench
duckdb:     ; ./run.sh duckdb
polars:     ; ./run.sh polars
spark:      ; ./run.sh spark
report:     ; ./run.sh report
help:       ; ./run.sh help

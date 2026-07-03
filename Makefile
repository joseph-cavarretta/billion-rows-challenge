.PHONY: install lint format check create_data pandas polars python sqlite duckdb awk convert duckdb-vortex polars-vortex rust-vortex

install:
	uv sync

lint:
	uv run ruff check .

format:
	uv run ruff format .

check:
	uv run ruff check .
	uv run ruff format --check .

DATA = src/data/stations.txt
VORTEX = src/data/measurements.vortex
ONE_BILLION = 1000000000
TIME = /usr/bin/time -p

define REQUIRE_DATA
	@if [ ! -f "$(DATA)" ]; then \
	  echo "ERROR: data file '$(DATA)' not found" >&2; \
	  echo "Hint: run 'make create_data' first."; \
	  exit 1; \
	fi
endef

define REQUIRE_VORTEX
	@if [ ! -f "$(VORTEX)" ]; then \
	  echo "ERROR: vortex file '$(VORTEX)' not found" >&2; \
	  echo "Hint: run 'make convert' first."; \
	  exit 1; \
	fi
endef

create_data:
	@$(TIME) uv run python src/scripts/create_data.py "$(ONE_BILLION)"

pandas:
	$(REQUIRE_DATA)
	@$(TIME) uv run python src/main/python/test_pandas.py "$(DATA)"

polars:
	$(REQUIRE_DATA)
	@$(TIME) uv run python src/main/python/test_polars.py "$(DATA)"

python:
	$(REQUIRE_DATA)
	@$(TIME) uv run python src/main/python/test_python.py "$(DATA)"

sqlite:
	$(REQUIRE_DATA)
	@uv run python src/main/python/test_sqlite.py load "$(DATA)"
	@$(TIME) uv run python src/main/python/test_sqlite.py query
	@uv run python src/main/python/test_sqlite.py cleanup

duckdb:
	$(REQUIRE_DATA)
	@$(TIME) uv run python src/main/python/test_duckdb.py "$(DATA)"

awk:
	$(REQUIRE_DATA)
	@$(TIME) ./src/main/bash/test_awk.sh "$(DATA)"

# --- Part 2: columnar formats (Parquet + Vortex) ---

# one-time: convert the CSV into measurements.parquet and measurements.vortex
convert:
	$(REQUIRE_DATA)
	@$(TIME) uv run python src/scripts/convert_data.py "$(DATA)"

duckdb-vortex:
	$(REQUIRE_VORTEX)
	@$(TIME) uv run python src/main/python/duckdb_vortex.py "$(VORTEX)"

polars-vortex:
	$(REQUIRE_VORTEX)
	@$(TIME) uv run python src/main/python/polars_vortex.py "$(VORTEX)"

rust-vortex:
	$(REQUIRE_VORTEX)
	@$(TIME) cargo run --release --manifest-path src/main/rust/Cargo.toml -- "$(VORTEX)"

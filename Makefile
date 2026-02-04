DATA = src/data/stations.txt
ONE_BILLION = 1000000000

define REQUIRE_DATA
	@if [ ! -f "$(DATA)" ]; then \
	  echo "ERROR: data file '$(DATA)' not found" >&2; \
	  echo "Hint: run 'make create_data' first."; \
	  exit 1; \
	fi
endef

create_data:
	@time python3 src/scripts/create_data.py "$(ONE_BILLION)"

pandas:
	$(REQUIRE_DATA)
	@time python3 src/main/python/test_pandas.py "$(DATA)"

polars:
	$(REQUIRE_DATA)
	@time python3 src/main/python/test_polars.py "$(DATA)"

python:
	$(REQUIRE_DATA)
	@time python3 src/main/python/test_python.py "$(DATA)"

sqlite:
	$(REQUIRE_DATA)
	@time python3 src/main/python/test_sqlite.py "$(DATA)"

duckdb:
	$(REQUIRE_DATA)
	@time python3 src/main/python/test_duckdb.py "$(DATA)"

awk:
	$(REQUIRE_DATA)
	@time ./src/main/bash/test_awk.sh "$(DATA)"


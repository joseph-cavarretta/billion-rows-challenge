create_data:
	@python3 src/scripts/create_data.py

pandas:
	@python3 src/main/test_pandas.py

polars:
	@python3 src/main/test_polars.py

python:
	@python3 src/main/test_python.py

sqlite:
	@python3 src/main/test_sqlite.py

duckdb:
	@python3 src/main/test_duckdb.py
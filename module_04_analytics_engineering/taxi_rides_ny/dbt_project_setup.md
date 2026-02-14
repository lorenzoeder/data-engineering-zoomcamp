# Steps by step to prepare project in codespace

cd to project directory

## 1. Install uv if not installed:
```shell
curl -LsSf https://astral.sh/uv/install.sh | sh
```
Then restart terminal

## 2. Create profiles file in codespace:
```shell
mkdir ~/.dbt
touch ~/.dbt/profiles.yml
nano ~/.dbt/profiles.yml
```

Then paste the desired profile in editor

## 3. Set up Python venv via uv:
```shell
uv init --python 3.13.10
```

## 4. Add dependencies:
```shell
uv add pandas numpy duckdb dbt-core dbt-duckdb
```

## 5. Sync installation:
```shell
uv sync
```

## 6. Activate venv:
```shell
source .venv/bin/activate
```

## 7. Confirm successful installs:
```shell
which python
dbt --version
python -c "import duckdb; print(duckdb.__version__)"
```

## 8. Confirm dbt connection:
```shell
dbt debug
```

## 9. Run ingestion script:
```shell
python ingest_data.py
```

(might take a while to create the table)

## 10. Build to prod:
```shell
dbt build --target prod
```

## 11. Confirm build by listing all objects in database:
```shell
python -c 'import duckdb; conn = duckdb.connect("taxi_rides_ny.duckdb"); print(conn.execute("SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables ORDER BY table_schema, table_name").fetchdf())'
```

## 12. When done, to clean up:
```shell
dbt clean
```

And **remove .duckdb file** (which contains every artifact that dbt created)

## 13. Deactivate venv:
```shell
deactivate
```

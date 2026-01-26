# Module 1 homework

## Question 1

```shell
docker run -it --entrypoint=bash python:3.13
```

In the Docker container:

```shell
pip -V
```

The answer is 25.3

## Question 2

To connect to the Postgres database created by Docker Compose, the pgadmin server needs to have hostname = the DB container name and port = the DB container port.

Therefore the answer is postgres:5432

## Setting up environment for questions 3-6

### Setting up the Python venv

```shell
uv init --python=3.13
```

```shell
uv run which python
```

```shell
uv add pandas pyarrow sqlalchemy psycopg2-binary tqdm click
```

### Building the Docker image

Dockerfile and Python ingestion script as in training module, but adapted for homework environment.

```shell
docker build -t green_taxi_data:v01 .
```

```shell
docker-compose up
```

Creating the server on pgadmin.

Ingesting the two tables (green taxi trips and zones):

```shell
docker run -it --rm --network=homework_default green_taxi_data:v01 --pg-user=postgres --pg-pass=postgres --pg-host=postgres --pg-port=5432 --pg-db=ny_taxi --target-table=green_taxi_data
```

## Question 3

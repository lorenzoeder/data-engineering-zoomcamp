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

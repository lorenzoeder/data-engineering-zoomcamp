#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


def run_taxi(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table):
    """Ingest NYC green taxi data into PostgreSQL database."""
    prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata'
    url = f'{prefix}_{year}-{month:02d}.parquet'

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_taxi = pd.read_parquet(url)

    df_taxi.to_sql(
        name=target_table,
        con=engine,
        if_exists='replace',
        index=False,
    )

def run_zones(pg_user, pg_pass, pg_host, pg_port, pg_db):
    """Ingest NYC taxi zones into PostgreSQL database."""
    zones_table_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_zones = pd.read_csv(zones_table_url)

    df_zones.to_sql(
    name='zones',
    con=engine,
    if_exists='replace',
    index=False
    )

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of the data')
@click.option('--month', default=11, type=int, help='Month of the data')
@click.option('--target-table', default='green_taxi_data', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table):
    """Ingest NYC taxi data into PostgreSQL database."""
    run_taxi(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table)
    run_zones(pg_user, pg_pass, pg_host, pg_port, pg_db)

if __name__ == '__main__':
    run()
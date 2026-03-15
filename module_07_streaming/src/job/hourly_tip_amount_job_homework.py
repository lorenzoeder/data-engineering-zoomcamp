from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            lpep_pickup_datetime VARCHAR,
            event_timestamp AS TRY_CAST(REPLACE(lpep_pickup_datetime, 'T', ' ') AS TIMESTAMP(3)),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_hourly_tip_sink(t_env):
    table_name = "processed_events_hourly_tips"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            total_tip_amount DOUBLE,
            PRIMARY KEY (window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_highest_tip_hour_sink(t_env):
    table_name = "processed_events_highest_tip_hour"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ranking_key INT,
            window_start TIMESTAMP(3),
            total_tip_amount DOUBLE,
            PRIMARY KEY (ranking_key) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_hourly_tip_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_events_source_kafka(t_env)
        hourly_sink = create_hourly_tip_sink(t_env)
        top_hour_sink = create_highest_tip_hour_sink(t_env)

        t_env.execute_sql(f"""
            CREATE TEMPORARY VIEW hourly_tips AS
            SELECT
                window_start,
                SUM(tip_amount) AS total_tip_amount
            FROM TABLE(
                TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
            )
            WHERE event_timestamp IS NOT NULL
            GROUP BY window_start
        """)

        statement_set = t_env.create_statement_set()

        statement_set.add_insert_sql(f"""
            INSERT INTO {hourly_sink}
            SELECT window_start, total_tip_amount
            FROM hourly_tips
        """)

        statement_set.add_insert_sql(f"""
            INSERT INTO {top_hour_sink}
            SELECT
                1 AS ranking_key,
                window_start,
                total_tip_amount
            FROM (
                SELECT
                    window_start,
                    total_tip_amount,
                    ROW_NUMBER() OVER (
                        ORDER BY total_tip_amount DESC, window_start ASC
                    ) AS rn
                FROM hourly_tips
            ) ranked
            WHERE rn = 1
        """)

        statement_set.execute().wait()

    except Exception as exc:
        print("Writing records from Kafka to JDBC failed:", str(exc))


if __name__ == '__main__':
    log_hourly_tip_aggregation()

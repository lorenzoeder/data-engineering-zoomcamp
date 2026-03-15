from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_distance DOUBLE,
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


def create_session_sink(t_env):
    table_name = "processed_events_sessionized"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (session_start, session_end, PULocationID) NOT ENFORCED
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


def create_longest_session_sink(t_env):
    table_name = "processed_events_longest_session"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ranking_key INT,
            PULocationID INT,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            num_trips BIGINT,
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


def log_session_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_events_source_kafka(t_env)
        session_sink = create_session_sink(t_env)
        longest_sink = create_longest_session_sink(t_env)

        # Session window with 5-minute inactivity gap per PULocationID.
        t_env.execute_sql(f"""
            CREATE TEMPORARY VIEW session_counts AS
            SELECT
                window_start AS session_start,
                window_end AS session_end,
                PULocationID,
                COUNT(*) AS num_trips
            FROM TABLE(
                SESSION(
                    TABLE {source_table} PARTITION BY PULocationID,
                    DESCRIPTOR(event_timestamp),
                    INTERVAL '5' MINUTE
                )
            )
            WHERE event_timestamp IS NOT NULL
            GROUP BY window_start, window_end, PULocationID
        """)

        statement_set = t_env.create_statement_set()

        statement_set.add_insert_sql(f"""
            INSERT INTO {session_sink}
            SELECT session_start, session_end, PULocationID, num_trips
            FROM session_counts
        """)

        statement_set.add_insert_sql(f"""
            INSERT INTO {longest_sink}
            SELECT
                1 AS ranking_key,
                PULocationID,
                session_start,
                session_end,
                num_trips
            FROM (
                SELECT
                    PULocationID,
                    session_start,
                    session_end,
                    num_trips,
                    ROW_NUMBER() OVER (
                        ORDER BY num_trips DESC, session_end DESC, PULocationID ASC
                    ) AS rn
                FROM session_counts
            ) ranked
            WHERE rn = 1
        """)

        statement_set.execute().wait()

    except Exception as exc:
        print("Writing records from Kafka to JDBC failed:", str(exc))


if __name__ == '__main__':
    log_session_aggregation()

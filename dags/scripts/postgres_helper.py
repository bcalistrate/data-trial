import pandas as pd
import scripts.constants as c
from sqlalchemy import create_engine, text

# Create SQLAlchemy engine
engine = create_engine(
    f"postgresql+psycopg2://{c.postgres_user}:{c.postgres_password}@{c.postgres_host}:{c.postgres_port}/{c.postgres_dbname}"
)


def run_sql(create_sql: str):
    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()
        conn.close()


def upload_overwrite_table(df: pd.DataFrame, table_name: str, schema_name: str):
    assert df.shape[0] > 0, f"{schema_name}.{table_name} is empty!"

    df.to_sql(
        name=table_name,
        schema=schema_name,
        con=engine,
        index=False,
        if_exists="replace",
    )


def read_table(table_name: str, schema_name: str):
    df = pd.read_sql_table(table_name=table_name, con=engine, schema=schema_name)
    assert df.shape[0] > 0, f"{schema_name}.{table_name} is empty!"

    return df


def read_query(query: str):
    df = pd.read_sql_query(sql=(query), con=engine)
    assert df.shape[0] > 0, "Query returned no results!"

    return df


def create_schema_if_not_exists(schema_name: str):
    assert isinstance(schema_name, str), "No schema name was provided!"

    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))

from src.utils.config import JDBC_URL, DB_PROPERTIES

def read_postgres(spark, table_name):
    """Reads a table from PostgreSQL into a Spark DataFrame."""
    return spark.read.jdbc(url=JDBC_URL, table=table_name, properties=DB_PROPERTIES)

def write_postgres(df, table_name, mode="overwrite"):
    """Writes a Spark DataFrame to a PostgreSQL table."""
    # Special handling for mode to ensure order/dependencies
    df.write.jdbc(url=JDBC_URL, table=table_name, mode=mode, properties=DB_PROPERTIES)

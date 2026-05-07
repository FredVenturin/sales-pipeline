import subprocess
from pathlib import Path

# Path to the bronze layer folder
BRONZE_PATH = Path("data/bronze")

# PostgreSQL connection settings
USER = "airflow"
DB = "airflow"

# Docker container name running PostgreSQL
CONTAINER = "sales-pipeline-postgres-1"


def search_bronze() -> Path:
    # Find all CSV files in the bronze folder
    files_csv = list(BRONZE_PATH.glob("*.csv"))

    # Raise error if no files found
    if not files_csv:
        raise FileNotFoundError("No CSV files found in Bronze layer. Run generate_data.py first.")

    # Return the most recent file based on filename (which contains the date)
    return max(files_csv, key=lambda f: f.name)


def create_bronze_table() -> None:
    # SQL command to drop and recreate the bronze table — ensures idempotency
    sql = """
        DROP TABLE IF EXISTS bronze_sales;
        CREATE TABLE bronze_sales (
            order_id      INTEGER,
            customer_name TEXT,
            product       TEXT,
            category      TEXT,
            quantity      INTEGER,
            unit_price    FLOAT,
            region        TEXT,
            status        TEXT,
            order_date    TEXT
        );
    """

    # Using subprocess instead of psycopg2 directly due to Windows CP1252 encoding issue.
    # Commands are executed inside the Linux container to avoid encoding conflicts.
    result = subprocess.run(
        ['docker', 'exec', '-i', CONTAINER,
         'psql', '-U', USER, '-d', DB, '-c', sql],
        capture_output=True,
        text=True,
        encoding='utf-8'
    )

    # Raise error if the command failed
    if result.returncode != 0:
        raise Exception(f"Failed to create table: {result.stderr}")

    print("Bronze table created successfully.")


def load_bronze(file_path: Path) -> None:
    # Open the CSV file and pipe it into PostgreSQL via COPY command
    # Using subprocess due to Windows CP1252 encoding issue
    with open(file_path, 'r', encoding='utf-8') as f:
        result = subprocess.run(
            ['docker', 'exec', '-i', CONTAINER,
             'psql', '-U', USER, '-d', DB,
             '-c', 'COPY bronze_sales FROM STDIN WITH CSV HEADER'],
            stdin=f,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )

    # Raise error if the command failed
    if result.returncode != 0:
        raise Exception(f"Failed to load bronze data: {result.stderr}")

    print(f"Bronze data loaded from {file_path.name}")


def run():
    # Find the most recent bronze CSV
    print("Searching bronze csv file...")
    file_path = search_bronze()

    # Create the bronze table in PostgreSQL
    print("Creating bronze table...")
    create_bronze_table()

    # Load the CSV data into the bronze table
    load_bronze(file_path)
    print("Bronze layer loaded successfully!")


if __name__ == "__main__":
    run()
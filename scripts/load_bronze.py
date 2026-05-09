import psycopg2
import csv
from pathlib import Path

# Path to the bronze layer folder inside the Docker container
BRONZE_PATH = Path("/opt/airflow/data/bronze")

# PostgreSQL connection settings
# host="postgres" uses the Docker service name — works inside the Airflow container
DB_HOST = "postgres"
DB_PORT = 5432
DB_USER = "airflow"
DB_PASSWORD = "airflow"
DB_NAME = "airflow"


def search_bronze() -> Path:
    # Find all CSV files in the bronze folder
    files_csv = list(BRONZE_PATH.glob("*.csv"))

    # Raise error if no files found — pipeline cannot continue without bronze data
    if not files_csv:
        raise FileNotFoundError("No CSV files found in Bronze layer. Run generate_data task first.")

    # Return the most recent file — filename contains the date (YYYY-MM-DD format)
    return max(files_csv, key=lambda f: f.name)


def create_bronze_table(cur) -> None:
    # Drop existing table (CASCADE removes dependent views like stg_sales) and recreate
    # dbt will recreate the views in the dbt_staging task
    cur.execute("""
        DROP TABLE IF EXISTS bronze_sales CASCADE;
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
    """)
    print("Bronze table created successfully.")


def load_data(cur, file_path: Path) -> None:
    # Open the CSV file for reading
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, quotechar='"', delimiter=',')

        # Skip the header row — we don't want to insert column names as data
        next(reader)

        # Insert each row into the bronze table
        for row in reader:
            # Skip rows that don't have exactly 9 columns
            if len(row) != 9:
                continue
            cur.execute(
                # %s are placeholders — psycopg2 safely substitutes each value
                "INSERT INTO bronze_sales VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                # Replace empty strings with None so they become NULL in the database
                [None if v == '' else v for v in row]
            )
    print(f"Bronze data loaded from {file_path.name}")


def run():
    # Step 1 — find the most recent bronze CSV
    print("Searching bronze csv file...")
    file_path = search_bronze()

    # Step 2 — open connection to PostgreSQL
    print("Connecting to PostgreSQL...")
    con = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )

    # Create a cursor to execute SQL commands
    cur = con.cursor()

    # Step 3 — create the bronze table
    print("Creating bronze table...")
    create_bronze_table(cur)

    # Step 4 — load CSV data into the table
    print("Loading bronze data...")
    load_data(cur, file_path)

    # Commit the transaction to persist all changes
    con.commit()

    # Close cursor and connection to free resources
    cur.close()
    con.close()
    print("Bronze layer loaded successfully!")


if __name__ == "__main__":
    run()
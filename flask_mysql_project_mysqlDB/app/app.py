# Import necessary libraries
from flask import Flask, request, jsonify
import mysql.connector
import os
import csv
import io
import requests

# Initialize the Flask application
app = Flask(__name__)


def get_db_connection():
    """
    Establishes and returns a connection to the MySQL database using environment variables.
    Defaults are provided for local development.

    env vars are defined in docker-compose.yml
    """
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "zoho"),
        password=os.getenv("DB_PASSWORD", "zoho"),
        database=os.getenv("DB_NAME", "domotz"),
    )


def create_table_from_csv(cursor, table_name, header):
    """
    Creates a table in the database with columns derived from the CSV header.
    Uses TEXT data type to avoid MySQL row size limit.
    """
    drop_table_sql = f"DROP TABLE IF EXISTS `{table_name}`;"
    cursor.execute(drop_table_sql)

    # Use TEXT instead of VARCHAR(255) to avoid row size error
    # Use MEDIUMTEXT and compressed row format to minimize inline storage
    columns = ", ".join(f"`{col}` MEDIUMTEXT" for col in header)
    create_table_sql = (
        f"CREATE TABLE `{table_name}` ({columns}) "
        f"ENGINE=InnoDB ROW_FORMAT=DYNAMIC;"
    )

    app.logger.debug(f"Creating table with SQL: {create_table_sql}")
    cursor.execute(create_table_sql)


@app.route("/")
def index():
    """
    Root endpoint that lists all tables in the connected MySQL database.
    """
    try:
        db = get_db_connection()
        cursor = db.cursor()
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()
        return jsonify({"tables": [table[0] for table in tables]})
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
    finally:
        if "cursor" in locals() and cursor:
            cursor.close()
        if "db" in locals() and db.is_connected():
            db.close()


@app.route("/update-db", methods=["POST"])
def update_db():
    """
    Upload CSV file from client, create table if needed, and insert data.
    """
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files["file"]
    table_name = request.form.get("table")

    if file.filename == "" or not table_name:
        return jsonify({"error": "File or table name missing"}), 400

    try:
        db = get_db_connection()
        cursor = db.cursor()
        stream = io.StringIO(file.stream.read().decode("utf-8"))
        reader = csv.reader(stream)
        header = next(reader)
        create_table_from_csv(cursor, table_name, header)
        num_columns = len(header)
        columns = ", ".join(f"`{col.strip()}`" for col in header)
        placeholders = ", ".join(["%s"] * num_columns)
        sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
        for row in reader:
            cursor.execute(sql, row)
        db.commit()
        return jsonify({"message": "Database updated successfully"})
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
    finally:
        if "cursor" in locals() and cursor:
            cursor.close()
        if "db" in locals() and db.is_connected():
            db.close()


@app.route("/update-db-local", methods=["POST"])
def update_db_local():
    """
    Upload data from a CSV file already on the server, using batch inserts to handle large files.
    for this version only accepts a single table at a time.
    example request body:
    {
        "filename": "data.csv",
        "table": "AccountsFinal"
    }
    """
    data = request.get_json()
    filename = data.get("filename")
    table_name = data.get("table")
    if not table_name or not filename:
        return jsonify({"error": "table name or file name missing"}), 400

    file_directory = os.getenv("CSV_DIRECTORY", "/shared")
    file_path = os.path.join(file_directory, filename)

    if not os.path.isfile(file_path):
        return jsonify({"error": f"File {filename} not found"}), 404

    BATCH_SIZE = 5000  # 1000
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            db = get_db_connection()
            cursor = db.cursor()
            reader = csv.reader(file)

            # Clean header first
            raw_header = next(reader)
            header = [
                col.strip() if col.strip() else f"column_{i}"
                for i, col in enumerate(raw_header)
            ]

            # Create table
            create_table_from_csv(cursor, table_name, header)

            # Prepare insert statement
            num_columns = len(header)
            columns = ", ".join(f"`{col}`" for col in header)
            placeholders = ", ".join(["%s"] * num_columns)
            sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

            # Insert in batches
            batch = []
            count = 0
            for row in reader:
                # Normalize row length
                if len(row) < num_columns:
                    row += [""] * (num_columns - len(row))
                elif len(row) > num_columns:
                    row = row[:num_columns]

                batch.append(row)
                count += 1

                if len(batch) >= BATCH_SIZE:
                    cursor.executemany(sql, batch)
                    db.commit()
                    app.logger.debug(f"{count} rows inserted...")
                    batch = []

            # Final batch
            if batch:
                cursor.executemany(sql, batch)
                db.commit()
                app.logger.debug(f"{count} rows inserted (final batch).")

            return jsonify(
                {
                    "message": f"Database updated successfully from {filename}",
                    "rows_inserted": count,
                }
            )

    except Exception as e:
        import traceback

        traceback.print_exc()
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
    finally:
        if "cursor" in locals() and cursor:
            cursor.close()
        if "db" in locals() and db.is_connected():
            db.close()


@app.route("/query", methods=["POST"])
def execute_query():
    """
    Accepts read-only SQL queries and executes them.
    Example request body:
    {
        "query": "SELECT * FROM `zoho`",
        "params": []
    }
    """
    data = request.get_json()
    sql_query = data.get("query", "").strip()
    params = data.get("params", [])

    if not sql_query:
        return jsonify({"error": "No query provided"}), 400

    if not any(
        sql_query.upper().startswith(op)
        for op in ("SELECT", "SHOW", "DESCRIBE", "EXPLAIN")
    ):
        return jsonify({"error": "Only read operations allowed"}), 403

    try:
        db = get_db_connection()
        cursor = db.cursor(dictionary=True)
        cursor.execute(sql_query, params)
        return jsonify(cursor.fetchall())
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
    finally:
        if "cursor" in locals() and cursor:
            cursor.close()
        if "db" in locals() and db.is_connected():
            db.close()


@app.route("/fetch-remote-file", methods=["POST"])
def fetch_remote_file():
    """
    Download a file from a remote URL using auth details from the request body.
    Saves the file as 'data.csv' in the server directory.
    Example request body:
    {
        "url": "https://example.com/data.csv",
        "Authorization": "Bearer your_auth_token",
        "ZANALYTICS-ORGID": "your_org_id",
        "filename": "AccountsFinal.csv"
    }
    """
    data = request.get_json()
    download_url = data.get("url")
    filename = data.get("filename")
    auth_header = data.get("Authorization")
    org_id_header = data.get("ZANALYTICS-ORGID")

    if not download_url or not auth_header or not org_id_header:
        return jsonify({"error": "Missing required parameters"}), 400

    headers = {"Authorization": auth_header, "ZANALYTICS-ORGID": org_id_header}

    try:
        response = requests.get(download_url, headers=headers, stream=True)
        response.raise_for_status()
        file_directory = os.getenv("DOWNLOAD_DIRECTORY", "/shared")
        os.makedirs(file_directory, exist_ok=True)
        file_path = os.path.join(file_directory, filename)  # Always overwrite
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return jsonify(
            {"message": "File downloaded successfully", "filename": f"{filename}"}
        )
    except requests.RequestException as e:
        return jsonify({"error": f"Request failed: {str(e)}"}), 500


# Run the app if executed directly
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)

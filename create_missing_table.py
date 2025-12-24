import os
import mysql.connector

# DB config should match what you use in run.py
# If you use environment variables on the server, this will pick them up.
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "tender_user"),
    "password": os.getenv("DB_PASSWORD", "StrongPassword@123"),
    "database": os.getenv("DB_NAME", "tender_automation_with_ai"),
    "autocommit": True,
    "charset": "utf8mb4",
    "use_unicode": True,
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS Main_Relevency (
    id INT AUTO_INCREMENT PRIMARY KEY,
    bid_number VARCHAR(100),
    query TEXT,
    detected_category VARCHAR(100),
    relevancy_score FLOAT,
    relevant TINYINT(1),
    best_match JSON,
    top_matches JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_bid_number (bid_number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""


def create_table():
    print("--- Database Setup Tool ---")
    print(f"Target Database: {DB_CONFIG['database']}")
    print(f"Target Host: {DB_CONFIG['host']}")

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("\nExecuting CREATE TABLE statement...")
        cursor.execute(CREATE_TABLE_SQL)
        print("✅ Table 'Main_Relevency' created successfully (or already exists).")

        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"\n❌ Database Error: {err}")
    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    create_table()

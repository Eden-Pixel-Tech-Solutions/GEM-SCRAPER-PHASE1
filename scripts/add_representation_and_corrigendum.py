import os
import mysql.connector

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "tender_automation_with_ai"),
    "autocommit": True,
}

def alter_table():
    print("--- Altering gem_tenders Table for JSON columns ---")
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Add Representation_json
        try:
            print("Adding column 'Representation_json'...")
            cursor.execute("ALTER TABLE gem_tenders ADD COLUMN Representation_json JSON DEFAULT NULL")
            print("✅ Column 'Representation_json' added.")
        except mysql.connector.Error as err:
            # Check for duplicate column error or try TEXT if JSON alias not supported (MariaDB/older MySQL)
            if err.errno == 1060: 
                print("⚠️ Column 'Representation_json' already exists.")
            else:
                print(f"⚠️ Error adding 'Representation_json' as JSON, trying TEXT: {err}")
                try:
                    cursor.execute("ALTER TABLE gem_tenders ADD COLUMN Representation_json TEXT DEFAULT NULL")
                    print("✅ Column 'Representation_json' added as TEXT.")
                except mysql.connector.Error as err2:
                     print(f"❌ Error adding 'Representation_json': {err2}")

        # Add Corrigendum_json
        try:
            print("Adding column 'Corrigendum_json'...")
            cursor.execute("ALTER TABLE gem_tenders ADD COLUMN Corrigendum_json JSON DEFAULT NULL")
            print("✅ Column 'Corrigendum_json' added.")
        except mysql.connector.Error as err:
            if err.errno == 1060:
                print("⚠️ Column 'Corrigendum_json' already exists.")
            else:
                 print(f"⚠️ Error adding 'Corrigendum_json' as JSON, trying TEXT: {err}")
                 try:
                    cursor.execute("ALTER TABLE gem_tenders ADD COLUMN Corrigendum_json TEXT DEFAULT NULL")
                    print("✅ Column 'Corrigendum_json' added as TEXT.")
                 except mysql.connector.Error as err2:
                     print(f"❌ Error adding 'Corrigendum_json': {err2}")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Database Connection Error: {e}")

if __name__ == "__main__":
    alter_table()

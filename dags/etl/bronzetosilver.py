import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL, isolation_level="AUTOCOMMIT")

def bronze_to_silver(engine):
    with engine.connect() as conn:

        # 1. Read raw data from Bronze
        raw_rows = conn.execute(text("""
            SELECT id, name, department, salary, created_at
            FROM bronze.raw_employees
        """)).fetchall()

        print(f"Fetched {len(raw_rows)} rows from bronze.raw_employees")

        # 2. Clean and transform
        cleaned_rows = []
        for row in raw_rows:
            if not row.name or not row.department:
                continue  # skip incomplete rows

            cleaned_rows.append({
                "name": row.name.strip(),
                "department": row.department.strip().upper(),
                "salary": row.salary,
            })

        print(f"Prepared {len(cleaned_rows)} cleaned rows")

        # 3. Insert into Silver
        for r in cleaned_rows:
            conn.execute(text("""
                INSERT INTO silver.cleaned_employees (name, department, salary)
                VALUES (:name, :department, :salary)
            """), r)

        print("Loaded cleaned data into silver.cleaned_employees")


if __name__ == "__main__":
    bronze_to_silver(engine)
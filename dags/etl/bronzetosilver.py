import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import datetime


load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL, isolation_level="AUTOCOMMIT")


def is_numeric(value):
    try:
        float(value)
        return True
    except:
        return False


def is_valid_date(value):
    try:
        datetime.datetime.fromisoformat(str(value))
        return True
    except:
        return False


def bronze_to_silver(engine):
    with engine.connect() as conn:

        # 1. Read raw data from Bronze
        raw_rows = conn.execute(text("""
            SELECT id, name, department, salary, created_at
            FROM bronze.raw_employees
        """)).fetchall()

        print(f"Fetched {len(raw_rows)} rows from bronze.raw_employees")

       cleaned_rows = []
        error_rows = []

        # 2. Validate + Clean
        for row in raw_rows:
            row_dict = dict(row)

            # Mandatory fields
            if not row_dict["name"] or not row_dict["department"] or not row_dict["salary"]:
                row_dict["error"] = "Missing mandatory fields"
                error_rows.append(row_dict)
                continue

            # Salary validation
            if not is_numeric(row_dict["salary"]):
                row_dict["error"] = "Invalid salary format"
                error_rows.append(row_dict)
                continue

            # Date validation
            if not is_valid_date(row_dict["created_at"]):
                row_dict["error"] = "Invalid date format"
                error_rows.append(row_dict)
                continue

            # Clean + Standardise
            cleaned_rows.append({
                "name": row_dict["name"].strip(),
                "department": row_dict["department"].strip().upper(),
                "salary": float(row_dict["salary"])
            })

        print(f"Prepared {len(cleaned_rows)} cleaned rows")
        print(f"Found {len(error_rows)} error rows")

        # 3. Insert error rows into Silver error table
        for err in error_rows:
            conn.execute(text("""
                INSERT INTO silver.error_employees
                (name, department, salary, error_message)
                VALUES (:name, :department, :salary, :error)
            """), {
                "name": err.get("name"),
                "department": err.get("department"),
                "salary": err.get("salary"),
                "error": err.get("error")
            })

       
        for r in cleaned_rows:
            conn.execute(text("""
                INSERT INTO silver.cleaned_employees (name, department, salary)
                VALUES (:name, :department, :salary)
                ON CONFLICT (name, department)
                DO UPDATE SET salary = EXCLUDED.salary;
            """), r)

        print("Loaded cleaned data into silver.cleaned_employees")


if __name__ == "__main__":
    bronze_to_silver(engine)

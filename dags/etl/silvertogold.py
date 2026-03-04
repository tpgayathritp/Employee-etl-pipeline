import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# Use autocommit so inserts persist
engine = create_engine(DATABASE_URL, isolation_level="AUTOCOMMIT")

def silver_to_gold(engine):
    with engine.connect() as conn:

        # 1. Read cleaned data from Silver
        rows = conn.execute(text("""
            SELECT department, salary
            FROM silver.cleaned_employees
        """)).fetchall()

        print(f"Fetched {len(rows)} rows from silver.cleaned_employees")

        # 2. Aggregate by department
        summary = {}
        for row in rows:
            dept = row.department
            salary = row.salary or 0

            if dept not in summary:
                summary[dept] = {"total_salary": 0, "employee_count": 0}

            summary[dept]["total_salary"] += salary
            summary[dept]["employee_count"] += 1

        print("Aggregated data:", summary)

        # 3. Clear existing Gold table (optional but common in ETL)
        conn.execute(text("TRUNCATE TABLE gold.employee_summary"))

        # 4. Insert aggregated results
        for dept, agg in summary.items():
            conn.execute(text("""
                INSERT INTO gold.employee_summary (department, total_salary, employee_count)
                VALUES (:department, :total_salary, :employee_count)
            """), {
                "department": dept,
                "total_salary": agg["total_salary"],
                "employee_count": agg["employee_count"]
            })

        print("Loaded aggregated data into gold.employee_summary")

if __name__ == "__main__":
    silver_to_gold(engine)
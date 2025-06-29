import unittest
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

DWH_DB_CONFIG = {
    'host': os.getenv('DWH_DB_HOST', 'localhost'),
    'user': os.getenv('DWH_DB_USER'),
    'password': os.getenv('DWH_DB_PASSWORD'),
    'database': os.getenv('DWH_DB_NAME', 'CarAnalyticsDWH'),
    'port': int(os.getenv('DWH_DB_PORT', 3306))
}

FAR_FUTURE_DATETIME_STR = "9999-12-31 23:59:59"

dwh_engine = None

def get_dwh_engine_for_test():
    """Creates and returns a SQLAlchemy engine for the DWH for testing."""
    global dwh_engine
    if dwh_engine is not None:
        return dwh_engine
    try:
        engine_url = (
            f"mysql+mysqlconnector://{DWH_DB_CONFIG['user']}:{DWH_DB_CONFIG['password']}@"
            f"{DWH_DB_CONFIG['host']}:{DWH_DB_CONFIG['port']}/{DWH_DB_CONFIG['database']}"
        )
        engine = create_engine(engine_url)
        with engine.connect() as connection:
            print(f"Successfully connected to DWH: {DWH_DB_CONFIG['database']} for testing.\n")
        dwh_engine = engine
        return dwh_engine
    except Exception as e:
        print(f"Error creating DWH engine for testing: {e}")
        raise ConnectionError(f"Failed to connect to DWH for testing: {e}")


def run_query_for_test(engine, query, params=None):
    try:
        with engine.connect() as connection:
            if params:
                result = pd.read_sql_query(sql=text(query), con=connection, params=params)
            else:
                result = pd.read_sql_query(sql=text(query), con=connection)
            return result
    except Exception as e:
        print(f"Error running query '{query[:50]}...': {e}")
        raise AssertionError(f"Query execution failed: {e}")
        return pd.DataFrame()


class TestDWHPopulation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = get_dwh_engine_for_test()
        if not cls.engine:
            raise unittest.SkipTest("DWH Engine could not be initialized. Skipping DWH tests.")

        cls.archive_tables = [
            'archive_accidents_csv', 'archive_customers', 'archive_vehicles',
            'archive_employees', 'archive_parts_inventory',
            'archive_service_appointments', 'archive_service_details'
        ]
        cls.dim_tables = [
            'dim_date', 'dim_location', 'dim_weather', 'dim_road_features',
            'dim_daylight', 'dim_customer', 'dim_vehicle', 'dim_employee',
            'dim_part', 'dim_service_type'
        ]
        cls.fact_tables = [
            'fact_accidents', 'fact_service_appointments', 'fact_service_parts_usage'
        ]
        cls.all_tables = cls.archive_tables + cls.dim_tables + cls.fact_tables

    def test_01_row_counts_not_zero_for_core_tables(self):
        print("\n--- Test: Row Counts ---")
        for table_name in self.all_tables:
            with self.subTest(table=table_name):
                df = run_query_for_test(self.engine, f"SELECT COUNT(*) as count FROM {table_name}")
                self.assertFalse(df.empty, f"Query for {table_name} count returned empty DataFrame.")
                count = df['count'].iloc[0]
                print(f"Table '{table_name}': {count} rows")
                self.assertGreater(count, 0, f"Table '{table_name}' has 0 rows, expected > 0 after initial load.")
        print("Row count checks: Passed (all tables have > 0 rows).\n")

    def test_02_fact_accidents_foreign_keys(self):
        print("\n--- Test: fact_accidents Foreign Keys ---")
        fk_columns = ['date_key', 'location_key', 'original_accident_id']
        for fk_col in fk_columns:
            with self.subTest(column=fk_col):
                df = run_query_for_test(self.engine, f"SELECT COUNT(*) as count FROM fact_accidents WHERE {fk_col} IS NULL")
                self.assertFalse(df.empty, f"Query for NULL FK {fk_col} in fact_accidents failed.")
                null_count = df['count'].iloc[0]
                self.assertEqual(null_count, 0, f"fact_accidents.{fk_col} contains {null_count} NULL values.")
        print("fact_accidents essential FK checks: Passed.\n")

    def test_03_fact_service_appointments_foreign_keys(self):
        print("\n--- Test: fact_service_appointments Foreign Keys ---")
        fk_columns = ['date_key', 'customer_key', 'vehicle_key', 'service_type_key', 'original_appointment_id']
        for fk_col in fk_columns:
            with self.subTest(column=fk_col):
                df = run_query_for_test(self.engine, f"SELECT COUNT(*) as count FROM fact_service_appointments WHERE {fk_col} IS NULL")
                self.assertFalse(df.empty, f"Query for NULL FK {fk_col} in fact_service_appointments failed.")
                null_count = df['count'].iloc[0]
                self.assertEqual(null_count, 0, f"fact_service_appointments.{fk_col} contains {null_count} NULL values.")
        print("fact_service_appointments essential FK checks: Passed.\n")

    def test_04_fact_service_parts_usage_foreign_keys(self):
        print("\n--- Test: fact_service_parts_usage Foreign Keys ---")
        fk_columns = ['date_key', 'vehicle_key', 'part_key', 'original_service_detail_id', 'original_appointment_id']
        for fk_col in fk_columns:
            with self.subTest(column=fk_col):
                df = run_query_for_test(self.engine, f"SELECT COUNT(*) as count FROM fact_service_parts_usage WHERE {fk_col} IS NULL")
                self.assertFalse(df.empty, f"Query for NULL FK {fk_col} in fact_service_parts_usage failed.")
                null_count = df['count'].iloc[0]
                self.assertEqual(null_count, 0, f"fact_service_parts_usage.{fk_col} contains {null_count} NULL values.")
        print("fact_service_parts_usage FK checks: Passed.\n")

    def test_05_spot_check_fact_accidents_join(self):
        print("\n--- Test: Spot Check fact_accidents Join ---")
        sample_fact_query = """
        SELECT fa.original_accident_id, dd.full_date, dl.city
        FROM fact_accidents fa
        JOIN dim_date dd ON fa.date_key = dd.date_key
        JOIN dim_location dl ON fa.location_key = dl.location_key
        ORDER BY RAND() LIMIT 1;
        """
        df_spot_check = run_query_for_test(self.engine, sample_fact_query)
        self.assertFalse(df_spot_check.empty, "Spot check query for fact_accidents returned no data.")
        self.assertEqual(len(df_spot_check), 1, "Spot check query for fact_accidents did not return exactly one row.")
        print("Sample joined fact_accidents record:")
        print(df_spot_check.to_string())
        print("Spot check fact_accidents: Passed.\n")

    def test_06_spot_check_fact_service_appointments_join(self):
        print("\n--- Test: Spot Check fact_service_appointments Join ---")
        sample_service_appt_query = """
        SELECT fsa.original_appointment_id, dc.customer_name, dv.make, dst.service_type_name
        FROM fact_service_appointments fsa
        JOIN dim_date dd ON fsa.date_key = dd.date_key
        JOIN dim_customer dc ON fsa.customer_key = dc.customer_key
        JOIN dim_vehicle dv ON fsa.vehicle_key = dv.vehicle_key
        JOIN dim_service_type dst ON fsa.service_type_key = dst.service_type_key
        ORDER BY RAND() LIMIT 1;
        """
        df_spot_check_appt = run_query_for_test(self.engine, sample_service_appt_query)
        self.assertFalse(df_spot_check_appt.empty, "Spot check query for fact_service_appointments returned no data.")
        self.assertEqual(len(df_spot_check_appt), 1, "Spot check query for fact_service_appointments did not return exactly one row.")
        print("Sample joined fact_service_appointments record:")
        print(df_spot_check_appt.to_string())
        print("Spot check fact_service_appointments: Passed.\n")


if __name__ == '__main__':
    print("=== Running Data Warehouse Unit Tests ===\n")
    unittest.main(verbosity=2)
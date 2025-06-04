import pandas as pd
from sqlalchemy import create_engine, text, inspect
from datetime import datetime, timedelta, date as datetime_date, time as datetime_time
import numpy as np
import os
from dotenv import load_dotenv
import traceback

load_dotenv()

SOURCE_DB_CONFIG = {
    'host': os.getenv('SOURCE_DB_HOST', 'localhost'),
    'user': os.getenv('SOURCE_DB_USER'),
    'password': os.getenv('SOURCE_DB_PASSWORD'),
    'database': os.getenv('SOURCE_DB_NAME', 'CarAnalyticsDB'),
    'port': int(os.getenv('SOURCE_DB_PORT', 3306))
}

DWH_DB_CONFIG = {
    'host': os.getenv('DWH_DB_HOST', 'localhost'),
    'user': os.getenv('DWH_DB_USER'),
    'password': os.getenv('DWH_DB_PASSWORD'),
    'database': os.getenv('DWH_DB_NAME', 'CarAnalyticsDWH'),
    'port': int(os.getenv('DWH_DB_PORT', 3306))
}

ACCIDENTS_CSV_PATH = 'datasets/US_Accidents_March23.csv'
CSV_CHUNK_SIZE = 50000
CSV_SUBSET_ROWS = 200000
DEFAULT_COUNTRY_OPERATIONAL = 'US'
FAR_FUTURE_DATETIME = datetime(9999, 12, 31, 23, 59, 59)

def get_db_engine(db_config):
    try:
        engine_url = (
            f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        engine = create_engine(engine_url)
        with engine.begin() as connection:
            print(f"Successfully connected to database: {db_config['database']}")
        return engine
    except Exception as e:
        print(f"Error creating engine for {db_config['database']}: {e}")
        return None

def get_table_columns(engine, table_name, schema=None):
    """Gets column names for a table."""
    try:
        inspector = inspect(engine)
        columns = [col['name'] for col in inspector.get_columns(table_name, schema=schema)]
        return columns
    except Exception as e:
        print(f"Error inspecting table {table_name}: {e}")
        return []

source_engine = get_db_engine(SOURCE_DB_CONFIG)
dwh_engine = get_db_engine(DWH_DB_CONFIG)


def populate_archive_accidents_csv(csv_path, engine_dwh, chunk_size=50000, row_limit=None):
    if not engine_dwh:
        print("DWH engine not available. Aborting archive_accidents_csv population.")
        return

    print(f"\n--- Populating archive_accidents_csv from {csv_path} ---")

    load_ts = datetime.now()
    all_inserted_count = 0

    try:
        for i, chunk_df_iter in enumerate(
                pd.read_csv(csv_path, chunksize=chunk_size, nrows=row_limit, low_memory=False)):
            print(f"Processing chunk {i + 1} for archive_accidents_csv...")
            chunk_df = chunk_df_iter.copy()

            chunk_df['load_timestamp'] = load_ts
            chunk_df['valid_from_timestamp'] = load_ts
            chunk_df['valid_to_timestamp'] = FAR_FUTURE_DATETIME

            rename_map = {
                'Distance(mi)': 'Distance_mi',
                'Temperature(F)': 'Temperature_F',
                'Wind_Chill(F)': 'Wind_Chill_F',
                'Humidity(%)': 'Humidity_pct',
                'Pressure(in)': 'Pressure_in',
                'Visibility(mi)': 'Visibility_mi',
                'Wind_Speed(mph)': 'Wind_Speed_mph',
                'Precipitation(in)': 'Precipitation_in'
            }
            chunk_df.rename(columns=rename_map, inplace=True)

            for dt_col in ['Start_Time', 'End_Time', 'Weather_Timestamp']:
                if dt_col in chunk_df.columns:
                    chunk_df[dt_col] = pd.to_datetime(chunk_df[dt_col], errors='coerce')

            bool_cols = ['Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit',
                         'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming',
                         'Traffic_Signal', 'Turning_Loop']
            for col in bool_cols:
                if col in chunk_df.columns:
                    chunk_df[col] = chunk_df[col].astype('boolean')

            try:
                with engine_dwh.begin() as connection:
                    chunk_df.to_sql('archive_accidents_csv', con=connection, if_exists='append', index=False)
                    connection.commit()
                print(f"Appended {len(chunk_df)} records to archive_accidents_csv from chunk {i + 1}.")
                all_inserted_count += len(chunk_df)
            except Exception as e_insert:
                print(f"Error inserting into archive_accidents_csv for chunk {i + 1}: {e_insert}")
                traceback.print_exc()

        print(
            f"--- archive_accidents_csv population from CSV complete. Total records inserted: {all_inserted_count} ---")

    except FileNotFoundError:
        print(f"Error: Accidents CSV file not found at {csv_path}")
    except Exception as e:
        print(f"An error occurred during archive_accidents_csv population: {e}")
        traceback.print_exc()


def populate_archive_customers_from_source_db(engine_source, engine_dwh):
    if not engine_source or not engine_dwh:
        print("Source or DWH engine not available. Aborting archive_customers population.")
        return

    print(f"\n--- Populating archive_customers from source CarAnalyticsDB ---")

    load_ts = datetime.now()
    try:
        with engine_source.connect() as s_conn, engine_dwh.begin() as d_conn:
            source_customers_df = pd.read_sql_query(
                "SELECT customer_id, first_name, last_name, email, phone_number, address, city, state, zip_code, registration_date, last_updated_timestamp FROM Customers",
                s_conn)

            if source_customers_df.empty:
                print("No customers found in source database.")
                return

            source_customers_df.rename(columns={
                'customer_id': 'original_customer_id',
                'last_updated_timestamp': 'source_last_updated_timestamp'
            }, inplace=True)

            source_customers_df['load_timestamp'] = load_ts
            source_customers_df['valid_from_timestamp'] = load_ts
            source_customers_df['valid_to_timestamp'] = FAR_FUTURE_DATETIME

            source_customers_df.to_sql('archive_customers', con=d_conn, if_exists='append', index=False)
            d_conn.commit()
            print(f"Successfully loaded/appended {len(source_customers_df)} records into archive_customers.")

    except Exception as e:
        print(f"An error occurred during archive_customers population: {e}")
        traceback.print_exc()


def populate_archive_vehicles_from_source_db(engine_source, engine_dwh):
    if not engine_source or not engine_dwh:
        print("Source or DWH engine not available. Aborting archive_vehicles population.")
        return

    print(f"\n--- Populating archive_vehicles from source CarAnalyticsDB ---")

    load_ts = datetime.now()
    try:
        with engine_source.connect() as s_conn, engine_dwh.begin() as d_conn:
            source_vehicles_df = pd.read_sql_query(
                "SELECT vehicle_id, customer_id, VIN, make, model, year, mileage, fuel_type, transmission_type, last_updated_timestamp FROM Vehicles",
                s_conn)

            if source_vehicles_df.empty:
                print("No vehicles found in source database.")
                return

            source_vehicles_df.rename(columns={
                'vehicle_id': 'original_vehicle_id',
                'last_updated_timestamp': 'source_last_updated_timestamp'
            }, inplace=True)

            source_vehicles_df['load_timestamp'] = load_ts
            source_vehicles_df['valid_from_timestamp'] = load_ts
            source_vehicles_df['valid_to_timestamp'] = FAR_FUTURE_DATETIME

            source_vehicles_df.to_sql('archive_vehicles', con=d_conn, if_exists='append', index=False)
            d_conn.commit()
            print(f"Successfully loaded/appended {len(source_vehicles_df)} records into archive_vehicles.")

    except Exception as e:
        print(f"An error occurred during archive_vehicles population: {e}")
        traceback.print_exc()


def populate_archive_employees_from_source_db(engine_source, engine_dwh):
    if not engine_source or not engine_dwh:
        print("Source or DWH engine not available. Aborting archive_employees population.")
        return

    print(f"\n--- Populating archive_employees from source CarAnalyticsDB ---")

    load_ts = datetime.now()
    try:
        with engine_source.connect() as s_conn, engine_dwh.begin() as d_conn:
            source_employees_df = pd.read_sql_query(
                "SELECT employee_id, first_name, last_name, role, hire_date, last_updated_timestamp FROM Employees",
                s_conn)

            if source_employees_df.empty:
                print("No employees found in source database.")
                return

            source_employees_df.rename(columns={
                'employee_id': 'original_employee_id',
                'last_updated_timestamp': 'source_last_updated_timestamp'
            }, inplace=True)

            source_employees_df['load_timestamp'] = load_ts
            source_employees_df['valid_from_timestamp'] = load_ts
            source_employees_df['valid_to_timestamp'] = FAR_FUTURE_DATETIME

            source_employees_df.to_sql('archive_employees', con=d_conn, if_exists='append', index=False)
            d_conn.commit()
            print(f"Successfully loaded/appended {len(source_employees_df)} records into archive_employees.")

    except Exception as e:
        print(f"An error occurred during archive_employees population: {e}")
        traceback.print_exc()


def populate_archive_parts_inventory_from_source_db(engine_source, engine_dwh):
    if not engine_source or not engine_dwh:
        print("Source or DWH engine not available. Aborting archive_parts_inventory population.")
        return

    print(f"\n--- Populating archive_parts_inventory from source CarAnalyticsDB ---")

    load_ts = datetime.now()
    try:
        with engine_source.connect() as s_conn, engine_dwh.begin() as d_conn:
            source_parts_df = pd.read_sql_query(
                "SELECT part_id, part_name, part_number, category, current_stock_quantity, unit_cost, last_updated_timestamp FROM PartsInventory",
                s_conn)

            if source_parts_df.empty:
                print("No parts found in source PartsInventory.")
                return

            source_parts_df.rename(columns={
                'part_id': 'original_part_id',
                'last_updated_timestamp': 'source_last_updated_timestamp'
            }, inplace=True)

            source_parts_df['load_timestamp'] = load_ts
            source_parts_df['valid_from_timestamp'] = load_ts
            source_parts_df['valid_to_timestamp'] = FAR_FUTURE_DATETIME

            source_parts_df.to_sql('archive_parts_inventory', con=d_conn, if_exists='append', index=False)
            d_conn.commit()
            print(f"Successfully loaded/appended {len(source_parts_df)} records into archive_parts_inventory.")

    except Exception as e:
        print(f"An error occurred during archive_parts_inventory population: {e}")
        traceback.print_exc()


def populate_archive_service_appointments_from_source_db(engine_source, engine_dwh):
    if not engine_source or not engine_dwh:
        print("Source or DWH engine not available. Aborting archive_service_appointments population.")
        return

    print(f"\n--- Populating archive_service_appointments from source CarAnalyticsDB ---")

    load_ts = datetime.now()
    try:
        with engine_source.connect() as s_conn, engine_dwh.begin() as d_conn:
            query = """
                    SELECT appointment_id,
                           vehicle_id,
                           customer_id,
                           appointment_date,
                           appointment_time,
                           service_type,
                           status,
                           technician_id,
                           total_labor_hours,
                           total_parts_cost,
                           total_service_cost,
                           notes,
                           created_at,
                           last_updated_timestamp
                    FROM ServiceAppointments
                    """
            source_appointments_df = pd.read_sql_query(query, s_conn)

            if source_appointments_df.empty:
                print("No service appointments found in source database.")
                return

            def format_time(time_val):
                if pd.isna(time_val):
                    return None
                if isinstance(time_val, datetime_time):
                    return time_val.strftime('%H:%M:%S')
                if isinstance(time_val, timedelta):
                    total_seconds = int(time_val.total_seconds())
                    hours = total_seconds // 3600
                    minutes = (total_seconds % 3600) // 60
                    seconds = total_seconds % 60
                    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                try:
                    return pd.to_datetime(str(time_val)).strftime('%H:%M:%S')
                except Exception:
                    return None

            source_appointments_df['appointment_time'] = source_appointments_df['appointment_time'].apply(format_time)

            if 'appointment_date' in source_appointments_df.columns:
                source_appointments_df['appointment_date'] = pd.to_datetime(
                    source_appointments_df['appointment_date'], errors='coerce').dt.date

            source_appointments_df.rename(columns={
                'appointment_id': 'original_appointment_id',
                'created_at': 'source_created_at',
                'last_updated_timestamp': 'source_last_updated_timestamp'
            }, inplace=True)

            source_appointments_df['load_timestamp'] = load_ts
            source_appointments_df['valid_from_timestamp'] = load_ts
            source_appointments_df['valid_to_timestamp'] = FAR_FUTURE_DATETIME

            source_appointments_df.to_sql('archive_service_appointments', con=d_conn, if_exists='append',
                                          index=False)
            d_conn.commit()
            print(
                f"Successfully loaded/appended {len(source_appointments_df)} records into archive_service_appointments.")

    except Exception as e:
        print(f"An error occurred during archive_service_appointments population: {e}")
        traceback.print_exc()


def populate_archive_service_details_from_source_db(engine_source, engine_dwh):
    if not engine_source or not engine_dwh:
        print("Source or DWH engine not available. Aborting archive_service_details population.")
        return

    print(f"\n--- Populating archive_service_details from source CarAnalyticsDB ---")

    load_ts = datetime.now()
    try:
        with engine_source.connect() as s_conn, engine_dwh.begin() as d_conn:
            query = """
                    SELECT service_detail_id,
                           appointment_id,
                           task_description,
                           labor_hours,
                           part_id,
                           quantity_used,
                           unit_cost_at_time_of_service,
                           created_at,
                           last_updated_timestamp
                    FROM ServiceDetails
                    """
            source_service_details_df = pd.read_sql_query(query, s_conn)

            if source_service_details_df.empty:
                print("No service details found in source database.")
                return

            source_service_details_df.rename(columns={
                'service_detail_id': 'original_service_detail_id',
                'created_at': 'source_created_at',
                'last_updated_timestamp': 'source_last_updated_timestamp'
            }, inplace=True)

            source_service_details_df['load_timestamp'] = load_ts
            source_service_details_df['valid_from_timestamp'] = load_ts
            source_service_details_df['valid_to_timestamp'] = FAR_FUTURE_DATETIME

            source_service_details_df.to_sql('archive_service_details', con=d_conn, if_exists='append', index=False)
            d_conn.commit()
            print(
                f"Successfully loaded/appended {len(source_service_details_df)} records into archive_service_details.")

    except Exception as e:
        print(f"An error occurred during archive_service_details population: {e}")
        traceback.print_exc()


def populate_dim_date_from_archive(engine_dwh, archive_accidents_table='archive_accidents_csv',
                                   archive_customers_table='archive_customers',
                                   archive_appointments_table='archive_service_appointments',
                                   archive_employees_table='archive_employees'):
    if not engine_dwh: return
    print(f"\n--- Populating dim_date from DWH archive tables ---")
    all_inserted_count = 0
    try:
        with engine_dwh.begin() as connection:
            existing_dates_df = pd.read_sql_query("SELECT DISTINCT full_date, hour FROM dim_date", connection)
            existing_dates_tuples = set()
            if not existing_dates_df.empty: existing_dates_tuples = set(
                existing_dates_df.apply(lambda r: (pd.Timestamp(r['full_date']).date(), r['hour']), axis=1))
            print(f"Found {len(existing_dates_tuples)} existing date-hour combinations in dim_date.")

            timestamps_series_list = []

            accidents_times_df = pd.read_sql_query(
                f"SELECT Start_Time, End_Time FROM {archive_accidents_table} WHERE Start_Time IS NOT NULL OR End_Time IS NOT NULL",
                connection)
            if not accidents_times_df.empty:
                timestamps_series_list.extend(
                    pd.to_datetime(accidents_times_df['Start_Time'], errors='coerce').tolist())
                timestamps_series_list.extend(pd.to_datetime(accidents_times_df['End_Time'], errors='coerce').tolist())
            else:
                print(f"No time data in {archive_accidents_table}.")

            customers_dates_df = pd.read_sql_query(
                f"SELECT registration_date FROM {archive_customers_table} WHERE registration_date IS NOT NULL",
                connection)
            if not customers_dates_df.empty:
                timestamps_series_list.extend(
                    pd.to_datetime(customers_dates_df['registration_date'], errors='coerce').tolist())
            else:
                print(f"No registration_date data in {archive_customers_table}.")

            appointments_dates_df = pd.read_sql_query(
                f"SELECT appointment_date, appointment_time FROM {archive_appointments_table} WHERE appointment_date IS NOT NULL",
                connection)
            if not appointments_dates_df.empty:
                for index, row in appointments_dates_df.iterrows():
                    dt = pd.to_datetime(row['appointment_date'], errors='coerce')
                    if pd.notna(dt):
                        hr = 0
                        if pd.notna(row['appointment_time']) and isinstance(row['appointment_time'], str):
                            try:
                                hr = datetime.strptime(row['appointment_time'], '%H:%M:%S').hour
                            except ValueError:
                                pass
                        elif isinstance(row['appointment_time'], datetime_time):
                            hr = row['appointment_time'].hour
                        timestamps_series_list.append(dt.replace(hour=hr, minute=0, second=0, microsecond=0))
            else:
                print(f"No appointment_date data in {archive_appointments_table}.")

            employees_dates_df = pd.read_sql_query(
                f"SELECT hire_date FROM {archive_employees_table} WHERE hire_date IS NOT NULL", connection)
            if not employees_dates_df.empty:
                timestamps_series_list.extend(pd.to_datetime(employees_dates_df['hire_date'], errors='coerce').tolist())
            else:
                print(f"No hire_date data in {archive_employees_table}.")

            if not timestamps_series_list: print("No valid timestamps from any archive source."); return

            timestamps_series = pd.Series(timestamps_series_list).dropna().unique()

            if len(timestamps_series) == 0: print(
                "No unique valid timestamps in archive after combining sources."); return

            date_dim_data = []
            for ts in timestamps_series:
                if not isinstance(ts, pd.Timestamp): ts = pd.Timestamp(ts)

                date_obj, hour_val = ts.date(), ts.hour

                if (date_obj, hour_val) not in existing_dates_tuples:
                    quarter = (ts.month - 1) // 3 + 1
                    date_dim_data.append(
                        {'full_date': date_obj, 'year': ts.year, 'month': ts.month, 'month_name': ts.strftime('%B'),
                         'day': ts.day, 'day_of_week': ts.strftime('%A'), 'quarter': quarter, 'hour': hour_val})
                    existing_dates_tuples.add((date_obj, hour_val))

            if not date_dim_data: print("No new unique date dimension data from combined archive sources."); return

            dim_date_df_to_load = pd.DataFrame(date_dim_data)
            dim_date_df_to_load.to_sql('dim_date', con=connection, if_exists='append', index=False)
            connection.commit()
            all_inserted_count = len(dim_date_df_to_load)
            print(f"Inserted {all_inserted_count} new records into dim_date from all archive sources.")
        print(
            f"--- dim_date population from all archive sources complete. Total new records inserted: {all_inserted_count} ---")
    except Exception as e:
        print(f"An error occurred during dim_date population: {e}");
        traceback.print_exc()


def populate_dim_location_from_archive(engine_dwh, archive_accidents_table='archive_accidents_csv',
                                       archive_customers_table='archive_customers'):
    if not engine_dwh: return
    print(
        f"\n--- Populating dim_location from DWH archive tables: {archive_accidents_table} and {archive_customers_table} ---")

    map_accidents = {'Start_Lat': 'latitude', 'Start_Lng': 'longitude', 'Street': 'street', 'City': 'city',
                     'County': 'county',
                     'State': 'state', 'Zipcode': 'zip_code', 'Country': 'country'}
    cols_accidents = list(map_accidents.keys())
    all_inserted_count = 0

    try:
        with engine_dwh.begin() as connection:
            existing_df = pd.read_sql_query("SELECT DISTINCT latitude, longitude, city, state FROM dim_location",
                                            connection)
            existing_tuples = set()
            if not existing_df.empty: existing_tuples = set(existing_df.apply(
                lambda r: (round(float(r['latitude']), 6) if pd.notna(r['latitude']) else None,
                           round(float(r['longitude']), 6) if pd.notna(r['longitude']) else None,
                           str(r['city']).strip().upper() if pd.notna(r['city']) else None,
                           str(r['state']).strip().upper() if pd.notna(r['state']) else None), axis=1))
            print(f"Found {len(existing_tuples)} existing location combinations in dim_location.")

            new_location_data = []

            archive_df_accidents = pd.read_sql_query(
                f"SELECT {', '.join(cols_accidents)} FROM {archive_accidents_table} WHERE City IS NOT NULL AND State IS NOT NULL",
                connection)
            if not archive_df_accidents.empty:
                archive_df_accidents.rename(columns=map_accidents, inplace=True)
                for c in ['street', 'city', 'county', 'state', 'zip_code', 'country']:
                    if c in archive_df_accidents.columns: archive_df_accidents[c] = archive_df_accidents[c].astype(
                        str).replace('nan', None).replace('None', None)
                if 'latitude' in archive_df_accidents.columns: archive_df_accidents['latitude'] = pd.to_numeric(
                    archive_df_accidents['latitude'], errors='coerce')
                if 'longitude' in archive_df_accidents.columns: archive_df_accidents['longitude'] = pd.to_numeric(
                    archive_df_accidents['longitude'], errors='coerce')
                archive_df_accidents.dropna(subset=['city', 'state'], inplace=True)

                for _, r in archive_df_accidents.iterrows():
                    lat = round(r['latitude'], 6) if pd.notna(r['latitude']) else None
                    lng = round(r['longitude'], 6) if pd.notna(r['longitude']) else None
                    city_val = str(r.get('city')).strip().upper() if pd.notna(r.get('city')) else None
                    state_val = str(r.get('state')).strip().upper() if pd.notna(r.get('state')) else None

                    if (lat, lng, city_val, state_val) not in existing_tuples:
                        new_location_data.append({
                            'street': str(r.get('street')).strip() if pd.notna(r.get('street')) else None,
                            'city': city_val,
                            'county': str(r.get('county')).strip() if pd.notna(r.get('county')) else None,
                            'state': state_val,
                            'zip_code': str(r.get('zip_code')).strip() if pd.notna(r.get('zip_code')) else None,
                            'country': str(r.get('country')).strip() if pd.notna(
                                r.get('country')) else DEFAULT_COUNTRY_OPERATIONAL,
                            'latitude': lat,
                            'longitude': lng
                        })
                        existing_tuples.add((lat, lng, city_val, state_val))
            else:
                print(f"No location data in {archive_accidents_table}.")

            archive_df_customers = pd.read_sql_query(
                f"SELECT address, city, state, zip_code FROM {archive_customers_table} WHERE city IS NOT NULL AND state IS NOT NULL",
                connection)
            if not archive_df_customers.empty:
                for _, r in archive_df_customers.iterrows():
                    lat, lng = None, None
                    city_val = str(r.get('city')).strip().upper() if pd.notna(r.get('city')) else None
                    state_val = str(r.get('state')).strip().upper() if pd.notna(r.get('state')) else None

                    if (lat, lng, city_val, state_val) not in existing_tuples:
                        new_location_data.append({
                            'street': str(r.get('address')).strip() if pd.notna(r.get('address')) else None,
                            'city': city_val,
                            'county': None,
                            'state': state_val,
                            'zip_code': str(r.get('zip_code')).strip() if pd.notna(r.get('zip_code')) else None,
                            'country': DEFAULT_COUNTRY_OPERATIONAL,
                            'latitude': lat,
                            'longitude': lng
                        })
                        existing_tuples.add((lat, lng, city_val, state_val))
            else:
                print(f"No customer location data in {archive_customers_table}.")

            if not new_location_data: print("No new unique location data from any archive source."); return

            load_df = pd.DataFrame(new_location_data).drop_duplicates()
            load_df.to_sql('dim_location', con=connection, if_exists='append', index=False)
            connection.commit()
            all_inserted_count = len(load_df)
            print(f"Inserted {all_inserted_count} new records into dim_location from all archive sources.")
        print(
            f"--- dim_location population from all archive sources complete. Total new records inserted: {all_inserted_count} ---")
    except Exception as e:
        print(f"An error occurred during dim_location population: {e}");
        traceback.print_exc()


def populate_dim_weather_from_archive(engine_dwh, archive_table_name='archive_accidents_csv'):
    if not engine_dwh: return
    print(f"\n--- Populating dim_weather from DWH archive table: {archive_table_name} ---")
    map_ = {'Weather_Condition': 'weather_condition', 'Temperature_F': 'temperature_f', 'Wind_Chill_F': 'wind_chill_f',
            'Humidity_pct': 'humidity_percent', 'Pressure_in': 'pressure_in', 'Visibility_mi': 'visibility_mi',
            'Wind_Direction': 'wind_direction', 'Wind_Speed_mph': 'wind_speed_mph',
            'Precipitation_in': 'precipitation_in'}
    cols, u_keys = list(map_.keys()), ['weather_condition', 'temperature_f', 'wind_speed_mph', 'visibility_mi']
    count = 0
    try:
        with engine_dwh.begin() as conn:
            exist_df = pd.read_sql_query(f"SELECT DISTINCT {', '.join(u_keys)} FROM dim_weather", conn)
            exist_set = set()
            if not exist_df.empty:
                for col_key in u_keys:
                    if pd.api.types.is_numeric_dtype(exist_df[col_key]):
                        exist_df[col_key] = pd.to_numeric(exist_df[col_key], errors='coerce')
                exist_set = set(
                    exist_df.apply(lambda r: tuple(r[c] if pd.notna(r[c]) else None for c in u_keys), axis=1))

            print(f"Found {len(exist_set)} existing weather combinations in dim_weather.")

            arch_df = pd.read_sql_query(f"SELECT {', '.join(cols)} FROM {archive_table_name}", conn)
            if arch_df.empty: print(f"No weather data in {archive_table_name}."); return

            arch_df.rename(columns=map_, inplace=True)
            num_cols_weather = ['temperature_f', 'wind_chill_f', 'humidity_percent', 'pressure_in', 'visibility_mi',
                                'wind_speed_mph', 'precipitation_in']
            for c in num_cols_weather:
                if c in arch_df.columns: arch_df[c] = pd.to_numeric(arch_df[c], errors='coerce')
            str_cols_weather = ['weather_condition', 'wind_direction']
            for c in str_cols_weather:
                if c in arch_df.columns: arch_df[c] = arch_df[c].astype(str).replace('nan', None).replace('None',
                                                                                                          None).str.strip()

            arch_df.dropna(subset=u_keys, how='all', inplace=True)
            if arch_df.empty: print("No valid weather data in archive after cleaning."); return

            new_data = []
            for _, r in arch_df.iterrows():
                current_key_list = []
                for c in u_keys:
                    val = r.get(c)
                    current_key_list.append(val if pd.notna(val) else None)
                key_tup = tuple(current_key_list)

                if key_tup not in exist_set:
                    record_to_add = {}
                    for db_c in map_.values():
                        val_rec = r.get(db_c)
                        record_to_add[db_c] = val_rec if pd.notna(val_rec) else None
                    new_data.append(record_to_add)
                    exist_set.add(key_tup)

            if not new_data: print("No new unique weather data from archive."); return

            load_df = pd.DataFrame(new_data).drop_duplicates()
            load_df.to_sql('dim_weather', con=conn, if_exists='append', index=False)
            conn.commit()
            count = len(load_df)
            print(f"Inserted {count} new records into dim_weather from {archive_table_name}.")
        print(f"--- dim_weather population from {archive_table_name} complete. Total new records inserted: {count} ---")
    except Exception as e:
        print(f"An error occurred: {e}");
        traceback.print_exc()


def populate_dim_road_features_from_archive(engine_dwh, archive_table_name='archive_accidents_csv'):
    if not engine_dwh: return
    print(f"\n--- Populating dim_road_features from DWH archive table: {archive_table_name} ---")
    arch_cols = ['Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station',
                 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']
    db_cols = ['has_amenity', 'has_bump', 'has_crossing', 'has_give_way', 'has_junction', 'has_no_exit', 'has_railway',
               'has_roundabout', 'has_station', 'has_stop', 'has_traffic_calming', 'has_traffic_signal',
               'is_turning_loop']
    count = 0
    try:
        with engine_dwh.begin() as conn:
            exist_df = pd.read_sql_query(f"SELECT DISTINCT {', '.join(db_cols)} FROM dim_road_features", conn)
            exist_set = set()
            if not exist_df.empty:
                for c in db_cols: exist_df[c] = exist_df[c].astype(bool)
                exist_set = set(exist_df.apply(lambda r: tuple(r[c] for c in db_cols), axis=1))
            print(f"Found {len(exist_set)} existing road feature combinations.")

            arch_df = pd.read_sql_query(f"SELECT {', '.join(arch_cols)} FROM {archive_table_name}", conn)
            if arch_df.empty: print(f"No road feature data in {archive_table_name}."); return

            arch_df.rename(columns=dict(zip(arch_cols, db_cols)), inplace=True)
            for c in db_cols: arch_df[c] = arch_df[c].fillna(False).astype(bool)

            unique_combos_df = arch_df[db_cols].drop_duplicates()
            if unique_combos_df.empty: print("No unique feature combinations in archive."); return

            new_data = []
            for _, r in unique_combos_df.iterrows():
                key_tup = tuple(r[c] for c in db_cols)
                if key_tup not in exist_set:
                    new_data.append({c: r[c] for c in db_cols})
                    exist_set.add(key_tup)

            if not new_data: print("No new unique road features from archive."); return

            load_df = pd.DataFrame(new_data)
            load_df.to_sql('dim_road_features', con=conn, if_exists='append', index=False)
            conn.commit()
            count = len(load_df)
            print(f"Inserted {count} new records into dim_road_features from {archive_table_name}.")
        print(
            f"--- dim_road_features population from {archive_table_name} complete. Total new records inserted: {count} ---")
    except Exception as e:
        print(f"An error occurred: {e}");
        traceback.print_exc()


def populate_dim_daylight_from_archive(engine_dwh, archive_table_name='archive_accidents_csv'):
    if not engine_dwh: return
    print(f"\n--- Populating dim_daylight from DWH archive table: {archive_table_name} ---")
    arch_cols = ['Sunrise_Sunset', 'Civil_Twilight', 'Nautical_Twilight', 'Astronomical_Twilight']
    db_cols = ['sunrise_sunset', 'civil_twilight', 'nautical_twilight', 'astronomical_twilight']
    count = 0
    try:
        with engine_dwh.begin() as conn:
            exist_df = pd.read_sql_query(f"SELECT DISTINCT {', '.join(db_cols)} FROM dim_daylight", conn)
            exist_set = set()
            if not exist_df.empty:
                for c in db_cols: exist_df[c] = exist_df[c].astype(str).replace('nan', None).replace('None',
                                                                                                     None).str.strip()
                exist_set = set(
                    exist_df.apply(lambda r: tuple(r[c] if pd.notna(r[c]) else None for c in db_cols), axis=1))
            print(f"Found {len(exist_set)} existing daylight combinations.")

            arch_df = pd.read_sql_query(f"SELECT {', '.join(arch_cols)} FROM {archive_table_name}", conn)
            if arch_df.empty: print(f"No daylight data in {archive_table_name}."); return

            arch_df.rename(columns=dict(zip(arch_cols, db_cols)), inplace=True)
            for c in db_cols: arch_df[c] = arch_df[c].astype(str).replace('nan', None).replace('None', None).str.strip()

            arch_df.dropna(subset=db_cols, how='all', inplace=True)
            unique_combos_df = arch_df[db_cols].drop_duplicates()
            if unique_combos_df.empty: print("No unique daylight combinations in archive."); return

            new_data = []
            for _, r in unique_combos_df.iterrows():
                key_tup = tuple(r[c] if pd.notna(r[c]) else None for c in db_cols)
                if key_tup not in exist_set:
                    new_data.append({c: (r[c] if pd.notna(r[c]) else None) for c in db_cols})
                    exist_set.add(key_tup)

            if not new_data: print("No new unique daylight data from archive."); return

            load_df = pd.DataFrame(new_data)
            load_df.to_sql('dim_daylight', con=conn, if_exists='append', index=False)
            conn.commit()
            count = len(load_df)
            print(f"Inserted {count} new records into dim_daylight from {archive_table_name}.")
        print(
            f"--- dim_daylight population from {archive_table_name} complete. Total new records inserted: {count} ---")
    except Exception as e:
        print(f"An error occurred: {e}");
        traceback.print_exc()


def populate_dim_customer_from_archive(engine_dwh, archive_table_name='archive_customers'):
    if not engine_dwh: return
    print(f"\n--- Populating dim_customer from DWH archive table: {archive_table_name} ---")
    try:
        with engine_dwh.begin() as conn:
            query = f"""
                SELECT original_customer_id, first_name, last_name, email,
                       phone_number, address, city, state, zip_code, registration_date
                FROM {archive_table_name}
                WHERE original_customer_id IS NOT NULL
            """
            arch_df = pd.read_sql_query(query, conn)
            if arch_df.empty: print(f"No customers in {archive_table_name} to process."); return

            arch_df.drop_duplicates(subset=['original_customer_id'], keep='first', inplace=True)

            load_list = []
            for _, r in arch_df.iterrows():
                load_list.append({'original_customer_id': r['original_customer_id'],
                                  'customer_name': f"{r.get('first_name', '')} {r.get('last_name', '')}".strip(),
                                  'email': r.get('email'),
                                  'phone_number': r.get('phone_number'),
                                  'address': r.get('address'), 'city': r.get('city'),
                                  'state': r.get('state'), 'zip_code': r.get('zip_code'),
                                  'registration_date': r.get('registration_date')})

            if not load_list: print("No customer data to load after deduplication."); return

            load_df = pd.DataFrame(load_list)
            load_df.to_sql('dim_customer', con=conn, if_exists='append', index=False)
            conn.commit()
            print(f"Successfully loaded/appended {len(load_df)} records into dim_customer from {archive_table_name}.")
    except Exception as e:
        print(f"An error occurred: {e}");
        traceback.print_exc()


def populate_dim_vehicle_from_archive(engine_dwh, archive_table_name='archive_vehicles'):
    if not engine_dwh: return
    print(f"\n--- Populating dim_vehicle from DWH archive table: {archive_table_name} ---")
    try:
        with engine_dwh.begin() as conn:
            query = f"""
                SELECT original_vehicle_id, VIN, make, model, year, fuel_type, transmission_type
                FROM {archive_table_name}
                WHERE original_vehicle_id IS NOT NULL AND VIN IS NOT NULL
            """
            arch_df = pd.read_sql_query(query, conn)
            if arch_df.empty: print(f"No vehicles in {archive_table_name} to process."); return

            arch_df.drop_duplicates(subset=['original_vehicle_id'], keep='first', inplace=True)
            arch_df.drop_duplicates(subset=['VIN'], keep='first', inplace=True)

            load_list = []
            for _, r in arch_df.iterrows():
                load_list.append({'original_vehicle_id': r['original_vehicle_id'], 'VIN': r['VIN'], 'make': r['make'],
                                  'model': r['model'], 'year': r['year'], 'fuel_type': r['fuel_type'],
                                  'transmission_type': r['transmission_type']})

            if not load_list: print("No vehicle data to load after deduplication."); return

            load_df = pd.DataFrame(load_list)
            load_df.to_sql('dim_vehicle', con=conn, if_exists='append', index=False)
            conn.commit()
            print(f"Successfully loaded/appended {len(load_df)} records into dim_vehicle from {archive_table_name}.")
    except Exception as e:
        print(f"An error occurred: {e}");
        traceback.print_exc()


def populate_dim_employee_from_archive(engine_dwh, archive_table_name='archive_employees'):
    if not engine_dwh: return
    print(f"\n--- Populating dim_employee from DWH archive table: {archive_table_name} ---")
    try:
        with engine_dwh.begin() as conn:
            query = f"""
                SELECT DISTINCT original_employee_id, first_name, last_name, role, hire_date 
                FROM {archive_table_name}
                WHERE original_employee_id IS NOT NULL
            """
            arch_df = pd.read_sql_query(query, conn)
            if arch_df.empty: print(f"No employees in {archive_table_name} to process."); return

            arch_df.drop_duplicates(subset=['original_employee_id'], keep='first', inplace=True)

            load_list = []
            for _, r in arch_df.iterrows():
                load_list.append({'original_employee_id': r['original_employee_id'],
                                  'employee_name': f"{r.get('first_name', '')} {r.get('last_name', '')}".strip(),
                                  'role': r.get('role'),
                                  'hire_date': r.get('hire_date')})

            if not load_list: print("No employee data to load after deduplication."); return

            load_df = pd.DataFrame(load_list)
            load_df.to_sql('dim_employee', con=conn, if_exists='append', index=False)
            conn.commit()
            print(f"Successfully loaded/appended {len(load_df)} records into dim_employee from {archive_table_name}.")
    except Exception as e:
        print(f"An error occurred: {e}");
        traceback.print_exc()


def populate_dim_part_from_archive(engine_dwh, archive_table_name='archive_parts_inventory'):
    if not engine_dwh:
        print("DWH engine not available. Aborting dim_part population.")
        return
    print(f"\n--- Populating dim_part from DWH archive table: {archive_table_name} (SCD Type 2 Initial) ---")
    try:
        with engine_dwh.begin() as connection:
            query = f"""
                SELECT 
                    original_part_id, 
                    part_name, 
                    part_number, 
                    category,
                    valid_from_timestamp AS archive_valid_from
                FROM {archive_table_name}
                WHERE original_part_id IS NOT NULL 
                  AND part_number IS NOT NULL
                  AND valid_to_timestamp = '{FAR_FUTURE_DATETIME.strftime('%Y-%m-%d %H:%M:%S')}'
            """
            arch_df = pd.read_sql_query(text(query), connection)

            if arch_df.empty:
                print(f"No current part records in {archive_table_name} to process for dim_part.")
                return

            arch_df.drop_duplicates(subset=['original_part_id'], keep='last', inplace=True)
            arch_df.drop_duplicates(subset=['part_number'], keep='last', inplace=True)

            load_list = []
            for _, r in arch_df.iterrows():
                part_data = {
                    'original_part_id': r['original_part_id'],
                    'part_name': r.get('part_name'),
                    'part_number': r.get('part_number'),
                    'category': r.get('category'),
                    'dim_valid_from': pd.to_datetime(r['archive_valid_from'], errors='coerce'),
                    'dim_valid_to': FAR_FUTURE_DATETIME,
                    'is_current': True
                }
                if pd.isna(part_data['dim_valid_from']):
                    print(
                        f"Warning: original_part_id {r['original_part_id']} has invalid archive_valid_from. Setting dim_valid_from to a default past date.")
                    part_data['dim_valid_from'] = datetime(1900, 1, 1)

                load_list.append(part_data)

            if not load_list:
                print("No part data to load into dim_part after processing.")
                return

            load_df = pd.DataFrame(load_list)

            dim_part_columns = get_table_columns(engine_dwh, 'dim_part')
            load_df = load_df[[col for col in load_df.columns if col in dim_part_columns]]

            if not load_df.empty:
                load_df.to_sql('dim_part', con=connection, if_exists='append', index=False, chunksize=1000)
                print(f"Successfully loaded/appended {len(load_df)} records into dim_part.")
            else:
                print("No records to insert into dim_part after column filtering.")
    except Exception as e:
        print(f"An error occurred during dim_part population: {e}")
        traceback.print_exc()

def populate_dim_service_type_from_archive(engine_dwh, archive_table_name='archive_service_appointments'):
    if not engine_dwh:
        print("DWH engine not available. Aborting dim_service_type from archive population.")
        return

    print(f"\n--- Populating dim_service_type from DWH archive table: {archive_table_name} ---")
    try:
        with engine_dwh.begin() as connection:
            query_archive = f"SELECT DISTINCT service_type FROM {archive_table_name} WHERE service_type IS NOT NULL AND TRIM(service_type) != ''"
            archive_service_types_df = pd.read_sql_query(query_archive, connection)

            if archive_service_types_df.empty:
                print(f"No distinct service types found in {archive_table_name}.")
                return

            service_types_to_load_df = archive_service_types_df.rename(columns={'service_type': 'service_type_name'})
            service_types_to_load_df['service_type_name'] = service_types_to_load_df['service_type_name'].str.strip()
            service_types_to_load_df.drop_duplicates(subset=['service_type_name'], inplace=True)

            existing_service_types_query = "SELECT service_type_name FROM dim_service_type"
            try:
                existing_df = pd.read_sql_query(existing_service_types_query, connection)
                existing_set = set(existing_df['service_type_name'].str.strip())
                print(f"Found {len(existing_set)} existing service types in dim_service_type.")
            except Exception:
                existing_set = set()
                print(
                    "dim_service_type is empty or error reading existing types. Will load all unique types from archive.")

            new_service_types_df = service_types_to_load_df[
                ~service_types_to_load_df['service_type_name'].isin(existing_set)]

            if not new_service_types_df.empty:
                new_service_types_df.to_sql('dim_service_type', con=connection, if_exists='append', index=False)
                connection.commit()
                print(
                    f"Successfully appended {len(new_service_types_df)} new records into dim_service_type from {archive_table_name}.")
            else:
                print(
                    "No new service types to append into dim_service_type from archive (all found already exist or no data).")

            print(f"Total distinct service types processed from {archive_table_name}: {len(service_types_to_load_df)}.")

    except Exception as e:
        print(f"An error occurred during dim_service_type population from {archive_table_name}: {e}")
        traceback.print_exc()


def populate_fact_accidents_from_archive(engine_dwh, archive_table_name='archive_accidents_csv'):
    if not engine_dwh:
        print("DWH engine not available. Aborting fact_accidents population.")
        return
    print(f"\n--- Populating fact_accidents from DWH archive table: {archive_table_name} ---")

    try:
        with engine_dwh.begin() as connection:
            print("Loading dimension tables for lookups...")
            dim_date_df = pd.read_sql_query("SELECT date_key, full_date, hour FROM dim_date", connection)
            dim_date_df['full_date'] = pd.to_datetime(dim_date_df['full_date']).dt.date

            dim_location_df = pd.read_sql_query(
                "SELECT location_key, latitude, longitude, city, state FROM dim_location", connection
            )
            dim_location_df['city'] = dim_location_df['city'].astype(str).str.upper().str.strip().replace('NAN',None)
            dim_location_df['state'] = dim_location_df['state'].astype(str).str.upper().str.strip().replace('NAN',None)
            dim_location_df['latitude'] = pd.to_numeric(dim_location_df['latitude'], errors='coerce').round(6)
            dim_location_df['longitude'] = pd.to_numeric(dim_location_df['longitude'], errors='coerce').round(6)

            dim_weather_df = pd.read_sql_query(
                "SELECT weather_key, weather_condition, temperature_f, wind_speed_mph, visibility_mi FROM dim_weather",
                connection
            )
            dim_weather_df['weather_condition'] = dim_weather_df['weather_condition'].astype(str).str.strip().replace('nan',None)
            dim_weather_df['temperature_f'] = pd.to_numeric(dim_weather_df['temperature_f'], errors='coerce')
            dim_weather_df['wind_speed_mph'] = pd.to_numeric(dim_weather_df['wind_speed_mph'], errors='coerce')
            dim_weather_df['visibility_mi'] = pd.to_numeric(dim_weather_df['visibility_mi'], errors='coerce')

            dim_road_features_df = pd.read_sql_query(
                "SELECT road_features_key, has_amenity, has_bump, has_crossing, has_give_way, has_junction, "
                "has_no_exit, has_railway, has_roundabout, has_station, has_stop, has_traffic_calming, "
                "has_traffic_signal, is_turning_loop FROM dim_road_features", connection
            )
            bool_cols_road_dim = [col for col in dim_road_features_df.columns if col.startswith('has_') or col.startswith('is_')]
            for col in bool_cols_road_dim:
                 dim_road_features_df[col] = dim_road_features_df[col].astype(bool)

            dim_daylight_df = pd.read_sql_query(
                "SELECT daylight_key, sunrise_sunset, civil_twilight, nautical_twilight, astronomical_twilight FROM dim_daylight",
                connection
            )
            daylight_cols_keys_in_dim = ['sunrise_sunset', 'civil_twilight', 'nautical_twilight', 'astronomical_twilight']
            for col in daylight_cols_keys_in_dim:
                 dim_daylight_df[col] = dim_daylight_df[col].astype(str).replace('nan', None).replace('None', None).str.strip()

            print("Loading archive_accidents_csv data...")
            archive_cols_needed = ['ID', 'Source', 'Severity', 'Start_Time', 'End_Time', 'Start_Lat', 'Start_Lng',
                                   'Distance_mi', 'Description', 'City', 'State',
                                   'Weather_Condition', 'Temperature_F', 'Wind_Speed_mph', 'Visibility_mi',
                                   'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway',
                                   'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop',
                                   'Sunrise_Sunset', 'Civil_Twilight', 'Nautical_Twilight', 'Astronomical_Twilight']
            archive_df = pd.read_sql_query(f"SELECT {', '.join(archive_cols_needed)} FROM {archive_table_name}", connection)

            if archive_df.empty:
                print(f"No data in {archive_table_name}. Skipping fact_accidents population.")
                return

            print(f"Processing {len(archive_df)} records from archive...")

            archive_df['Start_Time_dt'] = pd.to_datetime(archive_df['Start_Time'], errors='coerce')
            archive_df['End_Time_dt'] = pd.to_datetime(archive_df['End_Time'], errors='coerce')
            archive_df['join_date'] = archive_df['Start_Time_dt'].dt.date
            archive_df['join_hour'] = archive_df['Start_Time_dt'].dt.hour

            archive_df['join_lat'] = pd.to_numeric(archive_df['Start_Lat'], errors='coerce').round(6)
            archive_df['join_lng'] = pd.to_numeric(archive_df['Start_Lng'], errors='coerce').round(6)
            archive_df['join_city'] = archive_df['City'].astype(str).str.upper().str.strip().replace('NAN',None)
            archive_df['join_state'] = archive_df['State'].astype(str).str.upper().str.strip().replace('NAN',None)

            archive_df['Weather_Condition_join'] = archive_df['Weather_Condition'].astype(str).str.strip().replace('nan', None)
            archive_df['Temperature_F'] = pd.to_numeric(archive_df['Temperature_F'], errors='coerce')
            archive_df['Wind_Speed_mph'] = pd.to_numeric(archive_df['Wind_Speed_mph'], errors='coerce')
            archive_df['Visibility_mi'] = pd.to_numeric(archive_df['Visibility_mi'], errors='coerce')

            road_feature_archive_original_keys = ['Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway',
                                         'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']
            for col in road_feature_archive_original_keys:
                archive_df[col] = archive_df[col].fillna(False).astype(bool)

            daylight_archive_pascal_to_dim_snake_map = {
                'Sunrise_Sunset': 'sunrise_sunset',
                'Civil_Twilight': 'civil_twilight',
                'Nautical_Twilight': 'nautical_twilight',
                'Astronomical_Twilight': 'astronomical_twilight'
            }
            archive_daylight_join_cols = []
            for pascal_col, snake_col_dim_equivalent in daylight_archive_pascal_to_dim_snake_map.items():
                join_col_name = snake_col_dim_equivalent + '_join'
                archive_df[join_col_name] = archive_df[pascal_col].astype(str).replace('nan', None).replace('None', None).str.strip()
                archive_daylight_join_cols.append(join_col_name)

            print("Performing joins with dimension tables...")
            merged_df = archive_df.merge(dim_date_df, left_on=['join_date', 'join_hour'], right_on=['full_date', 'hour'], how='left')
            merged_df = merged_df.merge(dim_location_df,
                                        left_on=['join_lat', 'join_lng', 'join_city', 'join_state'],
                                        right_on=['latitude', 'longitude', 'city', 'state'],
                                        how='left', suffixes=('', '_loc'))
            merged_df = merged_df.merge(dim_weather_df,
                                        left_on=['Weather_Condition_join', 'Temperature_F', 'Wind_Speed_mph', 'Visibility_mi'],
                                        right_on=['weather_condition', 'temperature_f', 'wind_speed_mph', 'visibility_mi'],
                                        how='left', suffixes=('', '_weather'))
            merged_df = merged_df.merge(dim_road_features_df,
                                        left_on=road_feature_archive_original_keys,
                                        right_on=bool_cols_road_dim,
                                        how='left', suffixes=('', '_road'))
            merged_df = merged_df.merge(dim_daylight_df,
                                        left_on=archive_daylight_join_cols,
                                        right_on=daylight_cols_keys_in_dim,
                                        how='left', suffixes=('', '_daylight'))

            merged_df['duration_minutes'] = (merged_df['End_Time_dt'] - merged_df['Start_Time_dt']).dt.total_seconds() / 60.0
            merged_df['duration_minutes'] = merged_df['duration_minutes'].round(2)

            fact_df = merged_df[[
                'ID', 'date_key', 'location_key', 'weather_key', 'road_features_key', 'daylight_key',
                'Severity', 'Distance_mi', 'duration_minutes', 'Description', 'Source'
            ]].rename(columns={
                'ID': 'original_accident_id',
                'Severity': 'severity',
                'Distance_mi': 'distance_mi',
                'Description': 'description',
                'Source': 'source_system'
            })

            fact_df.dropna(subset=['original_accident_id', 'date_key', 'location_key'], inplace=True)

            print(f"Inserting {len(fact_df)} records into fact_accidents...")
            fact_df.to_sql('fact_accidents', con=connection, if_exists='append', index=False, chunksize=10000)
            connection.commit()
            print(f"--- fact_accidents population complete. Total records inserted: {len(fact_df)} ---")

    except Exception as e:
        print(f"An error occurred during fact_accidents population: {e}")
        traceback.print_exc()

def populate_fact_service_appointments_from_archive(engine_dwh, archive_table_name='archive_service_appointments'):
    if not engine_dwh:
        print("DWH engine not available. Aborting fact_service_appointments population.")
        return
    print(f"\n--- Populating fact_service_appointments from DWH archive table: {archive_table_name} ---")

    try:
        with engine_dwh.begin() as connection:
            print("Loading dimension tables for lookups...")
            dim_date_df = pd.read_sql_query("SELECT date_key, full_date, hour FROM dim_date", connection)
            dim_date_df['full_date'] = pd.to_datetime(dim_date_df['full_date']).dt.date

            dim_customer_df = pd.read_sql_query("SELECT customer_key, original_customer_id FROM dim_customer",
                                                connection)
            dim_vehicle_df = pd.read_sql_query("SELECT vehicle_key, original_vehicle_id FROM dim_vehicle", connection)
            dim_employee_df = pd.read_sql_query("SELECT employee_key, original_employee_id FROM dim_employee",
                                                connection)
            dim_service_type_df = pd.read_sql_query("SELECT service_type_key, service_type_name FROM dim_service_type",
                                                    connection)
            dim_service_type_df['service_type_name'] = dim_service_type_df['service_type_name'].astype(str).str.strip()

            print(f"Loading {archive_table_name} data...")
            archive_df = pd.read_sql_query(f"SELECT * FROM {archive_table_name}", connection)
            if archive_df.empty:
                print(f"No data in {archive_table_name}. Skipping fact_service_appointments population.")
                return

            print(f"Processing {len(archive_df)} records from archive...")

            archive_df['appointment_date_dt'] = pd.to_datetime(archive_df['appointment_date'], errors='coerce').dt.date

            def get_hour_from_time_str(time_val):
                if pd.isna(time_val): return 0
                if isinstance(time_val, datetime_time): return time_val.hour
                if isinstance(time_val, str):
                    try:
                        return datetime.strptime(time_val, '%H:%M:%S').hour
                    except ValueError:
                        return 0
                return 0

            archive_df['join_hour'] = archive_df['appointment_time'].apply(get_hour_from_time_str)

            archive_df['service_type_join'] = archive_df['service_type'].astype(str).str.strip()

            print("Performing joins with dimension tables...")
            merged_df = archive_df.merge(dim_date_df, left_on=['appointment_date_dt', 'join_hour'],
                                         right_on=['full_date', 'hour'], how='left')
            merged_df = merged_df.merge(dim_customer_df, left_on='customer_id', right_on='original_customer_id',
                                        how='left', suffixes=('', '_cust'))
            merged_df = merged_df.merge(dim_vehicle_df, left_on='vehicle_id', right_on='original_vehicle_id',
                                        how='left', suffixes=('', '_veh'))
            merged_df = merged_df.merge(dim_employee_df, left_on='technician_id', right_on='original_employee_id',
                                        how='left', suffixes=('', '_emp'))
            merged_df = merged_df.merge(dim_service_type_df, left_on='service_type_join', right_on='service_type_name',
                                        how='left', suffixes=('', '_stype'))

            fact_df = merged_df[[
                'original_appointment_id', 'date_key', 'customer_key', 'vehicle_key', 'employee_key',
                'service_type_key',
                'total_labor_hours', 'total_parts_cost', 'total_service_cost'
            ]].copy()

            measure_cols = ['total_labor_hours', 'total_parts_cost', 'total_service_cost']
            for col in measure_cols:
                fact_df[col] = pd.to_numeric(fact_df[col], errors='coerce').fillna(0)

            fact_df.dropna(
                subset=['original_appointment_id', 'date_key', 'customer_key', 'vehicle_key', 'service_type_key'],
                inplace=True)

            print(f"Inserting {len(fact_df)} records into fact_service_appointments...")
            fact_df.to_sql('fact_service_appointments', con=connection, if_exists='append', index=False,
                           chunksize=10000)
            connection.commit()
            print(f"--- fact_service_appointments population complete. Total records inserted: {len(fact_df)} ---")

    except Exception as e:
        print(f"An error occurred during fact_service_appointments population: {e}")
        traceback.print_exc()


def populate_fact_service_parts_usage_from_archive(engine_dwh, archive_details_table='archive_service_details',
                                                   archive_appointments_table='archive_service_appointments'):
    if not engine_dwh:
        print("DWH engine not available. Aborting fact_service_parts_usage population.")
        return
    print(
        f"\n--- Populating fact_service_parts_usage from DWH archive tables: {archive_details_table} and {archive_appointments_table} ---")

    try:
        with engine_dwh.begin() as connection:
            print("Loading dimension tables for lookups...")
            dim_date_df = pd.read_sql_query("SELECT date_key, full_date, hour FROM dim_date", connection)
            dim_date_df['full_date'] = pd.to_datetime(dim_date_df['full_date']).dt.date

            dim_vehicle_df = pd.read_sql_query("SELECT vehicle_key, original_vehicle_id FROM dim_vehicle", connection)
            dim_part_df = pd.read_sql_query("SELECT part_key, original_part_id FROM dim_part", connection)

            print(f"Loading {archive_details_table} data...")
            details_df = pd.read_sql_query(f"SELECT * FROM {archive_details_table}", connection)
            if details_df.empty:
                print(f"No data in {archive_details_table}. Skipping fact_service_parts_usage population.")
                return

            details_df = details_df[details_df['part_id'].notna()].copy()
            if details_df.empty:
                print(
                    f"No data with parts used in {archive_details_table}. Skipping fact_service_parts_usage population.")
                return

            print(f"Loading {archive_appointments_table} data for date and vehicle context...")
            appointments_df = pd.read_sql_query(
                f"SELECT original_appointment_id, vehicle_id, appointment_date, appointment_time FROM {archive_appointments_table}",
                connection)
            if appointments_df.empty:
                print(f"No data in {archive_appointments_table}. Cannot get context for parts usage.")
                return

            appointments_df['appointment_date_dt'] = pd.to_datetime(appointments_df['appointment_date'],
                                                                    errors='coerce').dt.date

            def get_hour_from_time_str(time_val):
                if pd.isna(time_val): return 0
                if isinstance(time_val, datetime_time): return time_val.hour
                if isinstance(time_val, str):
                    try:
                        return datetime.strptime(time_val, '%H:%M:%S').hour
                    except ValueError:
                        return 0
                return 0

            appointments_df['join_hour'] = appointments_df['appointment_time'].apply(get_hour_from_time_str)

            print(f"Processing {len(details_df)} service detail records with parts...")

            merged_details_df = details_df.merge(
                appointments_df[['original_appointment_id', 'vehicle_id', 'appointment_date_dt', 'join_hour']],
                left_on='appointment_id',
                right_on='original_appointment_id',
                how='left',
                suffixes=('_detail', '_appt')
            )

            unmatched_details_count = merged_details_df[
                'original_appointment_id'].isnull().sum()
            if unmatched_details_count > 0:
                print(
                    f"WARNING: {unmatched_details_count} service details could not be matched with service appointments.")

            merged_details_df.dropna(subset=['original_appointment_id'], inplace=True)
            if merged_details_df.empty:
                print("No service details could be matched with service appointments. Skipping.")
                return

            print("Performing joins with dimension tables...")
            merged_df = merged_details_df.merge(dim_date_df, left_on=['appointment_date_dt', 'join_hour'],
                                                right_on=['full_date', 'hour'], how='left')

            merged_df = merged_df.merge(dim_vehicle_df, left_on='vehicle_id', right_on='original_vehicle_id',
                                        how='left', suffixes=('', '_veh'))

            merged_df = merged_df.merge(dim_part_df, left_on='part_id', right_on='original_part_id', how='left',
                                        suffixes=('', '_part'))

            merged_df['quantity_used'] = pd.to_numeric(merged_df['quantity_used'], errors='coerce').fillna(0)
            merged_df['unit_cost_at_time_of_service'] = pd.to_numeric(merged_df['unit_cost_at_time_of_service'],
                                                                      errors='coerce').fillna(0)
            merged_df['total_cost_for_part_line'] = (
                        merged_df['quantity_used'] * merged_df['unit_cost_at_time_of_service']).round(2)

            fact_df_cols_to_select = [
                'original_service_detail_id', 'appointment_id',
                'date_key', 'vehicle_key', 'part_key',
                'quantity_used', 'unit_cost_at_time_of_service', 'total_cost_for_part_line'
            ]
            missing_cols = [col for col in fact_df_cols_to_select if col not in merged_df.columns]
            if missing_cols:
                print(f"ERROR: Columns missing from merged_df for fact_df creation: {missing_cols}")
                return

            fact_df = merged_df[fact_df_cols_to_select].copy()
            fact_df.rename(columns={'appointment_id': 'original_appointment_id'}, inplace=True)

            essential_keys = ['original_service_detail_id', 'original_appointment_id', 'date_key', 'vehicle_key',
                              'part_key']

            fact_df.dropna(subset=essential_keys, inplace=True)

            print(f"Inserting {len(fact_df)} records into fact_service_parts_usage...")
            if not fact_df.empty:
                fact_df.to_sql('fact_service_parts_usage', con=connection, if_exists='append', index=False,
                               chunksize=1000)
            print(f"--- fact_service_parts_usage population complete. Total records inserted: {len(fact_df)} ---")

    except Exception as e:
        print(f"An error occurred during fact_service_parts_usage population: {e}")
        traceback.print_exc()


def main_etl():
    print("Starting ETL pipeline...")

    if not source_engine or not dwh_engine:
        print("Database engine(s) not initialized. Aborting ETL.")
        return

    print("\n--- Phase 1: Archive Layer Population (Full Refresh) ---")
    try:
        with dwh_engine.connect() as connection:
            connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            print("Truncating all archive tables...")
            connection.execute(text("TRUNCATE TABLE archive_accidents_csv;"))
            connection.execute(text("TRUNCATE TABLE archive_customers;"))
            connection.execute(text("TRUNCATE TABLE archive_vehicles;"))
            connection.execute(text("TRUNCATE TABLE archive_employees;"))
            connection.execute(text("TRUNCATE TABLE archive_parts_inventory;"))
            connection.execute(text("TRUNCATE TABLE archive_service_appointments;"))
            connection.execute(text("TRUNCATE TABLE archive_service_details;"))
            connection.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
            connection.commit()
            print("All archive tables truncated.")
    except Exception as e_truncate:
        print(f"Error during DWH archive table truncation: {e_truncate}")
        traceback.print_exc()
        return

    populate_archive_accidents_csv(ACCIDENTS_CSV_PATH, dwh_engine, chunk_size=CSV_CHUNK_SIZE, row_limit=CSV_SUBSET_ROWS)
    populate_archive_customers_from_source_db(source_engine, dwh_engine)
    populate_archive_vehicles_from_source_db(source_engine, dwh_engine)
    populate_archive_employees_from_source_db(source_engine, dwh_engine)
    populate_archive_parts_inventory_from_source_db(source_engine, dwh_engine)
    populate_archive_service_appointments_from_source_db(source_engine, dwh_engine)
    populate_archive_service_details_from_source_db(source_engine, dwh_engine)
    print("\n--- Archive Layer Population Complete ---")

    print("\n--- Phase 2: Star Schema Layer Population (Full Refresh from Archive) ---")
    try:
        with dwh_engine.connect() as connection:
            connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            print("Truncating all star schema tables (Dimensions and Facts)...")
            connection.execute(text("TRUNCATE TABLE fact_accidents;"))
            connection.execute(text("TRUNCATE TABLE fact_service_appointments;"))
            connection.execute(text("TRUNCATE TABLE fact_service_parts_usage;"))
            connection.execute(text("TRUNCATE TABLE dim_date;"))
            connection.execute(text("TRUNCATE TABLE dim_location;"))
            connection.execute(text("TRUNCATE TABLE dim_weather;"))
            connection.execute(text("TRUNCATE TABLE dim_road_features;"))
            connection.execute(text("TRUNCATE TABLE dim_daylight;"))
            connection.execute(text("TRUNCATE TABLE dim_customer;"))
            connection.execute(text("TRUNCATE TABLE dim_vehicle;"))
            connection.execute(text("TRUNCATE TABLE dim_employee;"))
            connection.execute(text("TRUNCATE TABLE dim_part;"))
            connection.execute(text("TRUNCATE TABLE dim_service_type;"))
            connection.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
            connection.commit()
            print("All Star Schema Dim and Fact tables truncated.")
    except Exception as e_truncate:
        print(f"Error during DWH star schema table truncation: {e_truncate}")
        traceback.print_exc()
        return

    populate_dim_date_from_archive(dwh_engine)
    populate_dim_location_from_archive(dwh_engine)
    populate_dim_weather_from_archive(dwh_engine)
    populate_dim_road_features_from_archive(dwh_engine)
    populate_dim_daylight_from_archive(dwh_engine)
    populate_dim_customer_from_archive(dwh_engine)
    populate_dim_vehicle_from_archive(dwh_engine)
    populate_dim_employee_from_archive(dwh_engine)
    populate_dim_part_from_archive(dwh_engine)
    populate_dim_service_type_from_archive(dwh_engine)
    print("\n--- Dimension Table Population from Archive Complete ---")

    print("\n--- Phase 3: Fact Table Population from Archive ---")
    populate_fact_accidents_from_archive(dwh_engine)
    populate_fact_service_appointments_from_archive(dwh_engine)
    populate_fact_service_parts_usage_from_archive(dwh_engine)
    print("\n--- Fact Table Population from Archive Complete ---")

    print("\nETL pipeline finished (Archive, Dimension, and Fact layers populated).")


if __name__ == "__main__":
    main_etl()
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from datetime import datetime, timedelta, time as datetime_time
import os
from dotenv import load_dotenv
import traceback
import hashlib
import time

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

FAR_FUTURE_DATETIME = datetime(9999, 12, 31, 23, 59, 59)
ETL_PROCESS_NAME = 'INCREMENTAL_ETL'
LAST_STAR_SCHEMA_ETL_RUN_DATETIME = None


def get_db_engine(db_config, db_name_label="DB"):
    try:
        engine_url = (
            f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        engine = create_engine(engine_url)
        with engine.connect() as connection:
            print(f"Successfully connected to {db_name_label}: {db_config['database']}")
        return engine
    except Exception as e:
        print(f"Error creating engine for {db_name_label} ({db_config['database']}): {e}")
        return None


def get_table_columns(engine, table_name, schema=None):
    try:
        inspector = inspect(engine)
        columns = [col['name'] for col in inspector.get_columns(table_name, schema=schema)]
        return columns
    except Exception as e:
        print(f"Error inspecting table {table_name}: {e}")
        return []


def format_time(time_val):
    if pd.isna(time_val): return None
    if isinstance(time_val, datetime_time): return time_val.strftime('%H:%M:%S')
    if isinstance(time_val, timedelta):
        total_seconds = int(time_val.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    try:
        if isinstance(time_val, str) and ':' in time_val:
            return time_val
        return pd.to_datetime(str(time_val)).strftime('%H:%M:%S')
    except Exception:
        return None


def incremental_load_archive_table(
        engine_source, engine_dwh, source_table_name, archive_table_name,
        source_pk_cols, archive_original_id_col, source_to_archive_col_map,
        cols_to_compare_for_changes, current_etl_datetime
):
    print(
        f"\n--- Starting incremental load for archive table: {archive_table_name} from source: {source_table_name} ---")
    closing_timestamp = current_etl_datetime - timedelta(microseconds=1)

    def add_change_hash(df, cols):
        cols_to_hash = sorted([c for c in cols if c in df.columns])
        if not cols_to_hash:
            df['_change_hash'] = pd.Series(dtype='str')
            return df
        temp_df = pd.DataFrame(index=df.index)
        for col in cols_to_hash:
            series = df[col]
            if pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_integer_dtype(series):
                temp_df[col] = series.round(4).astype(str)
            elif pd.api.types.is_datetime64_any_dtype(series):
                temp_df[col] = series.dt.strftime('%Y-%m-%d %H:%M:%S')
            elif pd.api.types.is_timedelta64_dtype(series):
                temp_df[col] = series.apply(lambda
                                                x: f"{int(x.total_seconds()) // 3600:02d}:{(int(x.total_seconds()) % 3600) // 60:02d}:{int(x.total_seconds()) % 60:02d}" if pd.notna(
                    x) else "")
            else:
                temp_df[col] = series.astype(str).str.replace('\r\n', '\n', regex=False).str.strip()
        temp_df.fillna('__NA__', inplace=True)

        def hash_row(row):
            composite_key = '|'.join(row)
            return hashlib.md5(composite_key.encode()).hexdigest()

        if not temp_df.empty:
            df['_change_hash'] = temp_df.apply(hash_row, axis=1)
        else:
            df['_change_hash'] = pd.Series(dtype='str')
        return df

    source_df = pd.read_sql_query(f"SELECT * FROM {source_table_name}", engine_source)
    if not source_df.empty:
        source_df.dropna(subset=source_pk_cols, inplace=True)

    if source_table_name == "ServiceAppointments" and 'appointment_time' in source_df.columns:
        source_df['appointment_time'] = source_df['appointment_time'].apply(format_time)

    current_archive_df = pd.read_sql_query(
        text(f"SELECT * FROM {archive_table_name} WHERE valid_to_timestamp = :far_future"),
        engine_dwh, params={'far_future': FAR_FUTURE_DATETIME}
    )

    if source_table_name == "ServiceAppointments" and 'appointment_time' in current_archive_df.columns:
        current_archive_df['appointment_time'] = current_archive_df['appointment_time'].apply(format_time)

    source_df_renamed = source_df.rename(columns=source_to_archive_col_map)
    source_df_renamed = add_change_hash(source_df_renamed, cols_to_compare_for_changes)
    current_archive_df = add_change_hash(current_archive_df, cols_to_compare_for_changes)

    merged_df = pd.merge(
        source_df_renamed, current_archive_df, on=archive_original_id_col,
        how='outer', suffixes=('_source', '_archive'), indicator=True
    )

    new_records_df = merged_df[merged_df['_merge'] == 'left_only']
    deleted_records_df = merged_df[merged_df['_merge'] == 'right_only']
    updated_records_df = merged_df[
        (merged_df['_merge'] == 'both') &
        (merged_df['_change_hash_source'] != merged_df['_change_hash_archive'])
        ]

    print(f"Identified {len(new_records_df)} new records for {archive_table_name}.")
    print(f"Identified {len(deleted_records_df)} records deleted from source for {archive_table_name}.")
    print(f"Identified {len(updated_records_df)} updated records for {archive_table_name}.")

    records_to_close_ids = pd.concat([
        deleted_records_df[archive_original_id_col],
        updated_records_df[archive_original_id_col]
    ]).unique().tolist()

    all_ids_to_insert = pd.concat([
        new_records_df[archive_original_id_col],
        updated_records_df[archive_original_id_col]
    ]).unique().tolist()

    records_to_insert_df = pd.DataFrame()
    if all_ids_to_insert:
        records_to_insert_df = source_df_renamed[
            source_df_renamed[archive_original_id_col].isin(all_ids_to_insert)
        ].copy()

    if not records_to_insert_df.empty:
        records_to_insert_df['load_timestamp'] = current_etl_datetime
        records_to_insert_df['valid_from_timestamp'] = current_etl_datetime
        records_to_insert_df['valid_to_timestamp'] = FAR_FUTURE_DATETIME

    if records_to_close_ids or not records_to_insert_df.empty:
        try:
            with engine_dwh.begin() as connection:
                if records_to_close_ids:
                    print(f"Closing out {len(records_to_close_ids)} old/deleted records in {archive_table_name}...")
                    chunk_size_update = 500
                    for i in range(0, len(records_to_close_ids), chunk_size_update):
                        id_chunk = records_to_close_ids[i:i + chunk_size_update]
                        placeholders = ', '.join([f':id_{j}' for j in range(len(id_chunk))])
                        params_for_close = {f'id_{j}': id_val for j, id_val in enumerate(id_chunk)}
                        params_for_close.update(
                            {'closing_ts': closing_timestamp, 'far_future': FAR_FUTURE_DATETIME}
                        )
                        update_stmt = text(
                            f"UPDATE {archive_table_name} SET valid_to_timestamp = :closing_ts WHERE {archive_original_id_col} IN ({placeholders}) AND valid_to_timestamp = :far_future"
                        )
                        connection.execute(update_stmt, params_for_close)

                if not records_to_insert_df.empty:
                    print(f"Inserting {len(records_to_insert_df)} new/updated records into {archive_table_name}...")
                    all_archive_cols = get_table_columns(engine_dwh, archive_table_name)
                    insert_df = records_to_insert_df[
                        [col for col in records_to_insert_df.columns if col in all_archive_cols]]
                    insert_df.to_sql(archive_table_name, con=connection, if_exists='append', index=False,
                                     chunksize=1000)
                print(f"Successfully committed changes for {archive_table_name}.")
        except Exception as e:
            print(f"Error during DB operations for {archive_table_name}, transaction rolled back: {e}")
            traceback.print_exc()
    else:
        print(f"No changes to apply to {archive_table_name}.")

    print(f"--- Incremental load for archive table: {archive_table_name} finished ---")

def _generic_scd2_update(
        connection, dim_table_name, archive_table_name, original_id_col, dim_key_col,
        last_etl_run_datetime, current_etl_datetime, record_mapper_func
):
    far_future_dim = FAR_FUTURE_DATETIME
    closing_timestamp = current_etl_datetime - timedelta(microseconds=1)

    changed_archive_query = text(f"""
            SELECT DISTINCT {original_id_col}
            FROM {archive_table_name}
            WHERE valid_from_timestamp > :last_run AND valid_from_timestamp <= :current_run
        """)
    unique_changed_ids_df = pd.read_sql_query(
        changed_archive_query, connection,
        params={'last_run': last_etl_run_datetime, 'current_run': current_etl_datetime, 'far_future': far_future_dim}
    )

    if unique_changed_ids_df.empty:
        print(f"No new/updated versions from {archive_table_name} to process for {dim_table_name}.")
        return

    unique_changed_ids = unique_changed_ids_df[original_id_col].tolist()
    print(f"Found {len(unique_changed_ids)} unique entities with changes in {archive_table_name} for {dim_table_name}.")

    current_dim_df = pd.read_sql_query(
        f"SELECT {dim_key_col}, {original_id_col} FROM {dim_table_name} WHERE is_current = TRUE", connection
    )
    current_dim_map = pd.Series(current_dim_df[dim_key_col].values, index=current_dim_df[original_id_col]).to_dict()

    records_to_insert = []
    keys_to_close = []

    for original_id in unique_changed_ids:
        latest_active_archive_row_df = pd.read_sql_query(
            text(
                f"SELECT * FROM {archive_table_name} WHERE {original_id_col} = :id AND valid_to_timestamp = :far_future ORDER BY valid_from_timestamp DESC LIMIT 1"),
            connection, params={'id': original_id, 'far_future': far_future_dim}
        )
        is_deleted_from_source = latest_active_archive_row_df.empty

        if original_id in current_dim_map:
            key_to_close = current_dim_map[original_id]

            if is_deleted_from_source:
                last_known_version_df = pd.read_sql_query(
                    text(
                        f"SELECT valid_to_timestamp FROM {archive_table_name} WHERE {original_id_col} = :id ORDER BY valid_from_timestamp DESC LIMIT 1"),
                    connection, params={'id': original_id}
                )
                closure_date = pd.to_datetime(last_known_version_df['valid_to_timestamp'].iloc[0])
                keys_to_close.append({'key': key_to_close, 'valid_to': closure_date})

            else:
                archive_row = latest_active_archive_row_df.iloc[0]
                keys_to_close.append({'key': key_to_close,
                                      'valid_to': pd.to_datetime(archive_row['valid_from_timestamp']) - timedelta(
                                          microseconds=1)})
                records_to_insert.append(record_mapper_func(archive_row))

        elif not is_deleted_from_source:
            archive_row = latest_active_archive_row_df.iloc[0]
            records_to_insert.append(record_mapper_func(archive_row))

    if keys_to_close:
        print(f"Closing out {len(keys_to_close)} old versions in {dim_table_name}...")
        for item in keys_to_close:
            stmt_close_dim = text(
                f"UPDATE {dim_table_name} SET is_current = FALSE, dim_valid_to = :new_dim_valid_to WHERE {dim_key_col} = :key AND is_current = TRUE")
            connection.execute(stmt_close_dim, {'new_dim_valid_to': item['valid_to'], 'key': int(item['key'])})

    if records_to_insert:
        print(f"Inserting {len(records_to_insert)} new versions into {dim_table_name}...")
        insert_df = pd.DataFrame(records_to_insert)
        dim_cols = get_table_columns(connection.engine, dim_table_name)
        insert_df = insert_df[[col for col in insert_df.columns if col in dim_cols]]
        insert_df.to_sql(dim_table_name, con=connection, if_exists='append', index=False, chunksize=1000)

    print(f"Successfully processed changes for {dim_table_name}.")

def incremental_update_dim_date(engine_dwh, current_etl_datetime, last_etl_run_datetime):
    print("\n--- Starting incremental update for dim_date ---")
    archive_appointments_table = 'archive_service_appointments'
    try:
        with engine_dwh.begin() as connection:
            existing_dates_df = pd.read_sql_query("SELECT full_date, hour FROM dim_date", connection)
            existing_dates_df['full_date'] = pd.to_datetime(existing_dates_df['full_date']).dt.date
            existing_tuples = set(zip(existing_dates_df['full_date'], existing_dates_df['hour']))

            new_appts_dates_query = text(f"""
                SELECT DISTINCT appointment_date, appointment_time FROM {archive_appointments_table}
                WHERE valid_from_timestamp >= :last_run AND valid_from_timestamp <= :current_run
            """)
            new_appts_dates_df = pd.read_sql_query(
                new_appts_dates_query, connection,
                params={'last_run': last_etl_run_datetime, 'current_run': current_etl_datetime}
            )

            if new_appts_dates_df.empty:
                print("No new appointment dates to process for dim_date.")
                return

            new_date_dim_data = []

            for _, row in new_appts_dates_df.iterrows():
                date_obj = row['appointment_date']
                time_obj = row['appointment_time']

                if pd.isna(date_obj): continue

                hour_val = 0
                if pd.notna(time_obj):
                    if isinstance(time_obj, timedelta):
                        hour_val = int(time_obj.total_seconds() // 3600)
                    elif isinstance(time_obj, str):
                        try:
                            hour_val = datetime.strptime(time_obj, '%H:%M:%S').hour
                        except (ValueError, TypeError):
                            hour_val = 0

                if (date_obj, hour_val) not in existing_tuples:
                    ts = datetime.combine(date_obj, datetime.min.time()).replace(hour=hour_val)
                    quarter = (ts.month - 1) // 3 + 1
                    new_date_dim_data.append({
                        'full_date': date_obj, 'year': ts.year, 'month': ts.month, 'month_name': ts.strftime('%B'),
                        'day': ts.day, 'day_of_week': ts.strftime('%A'), 'quarter': quarter, 'hour': hour_val
                    })
                    existing_tuples.add((date_obj, hour_val))

            if not new_date_dim_data:
                print("No new unique date-hour combinations to add to dim_date.")
                return

            load_df = pd.DataFrame(new_date_dim_data).drop_duplicates()
            print(f"Inserting {len(load_df)} new records into dim_date.")
            load_df.to_sql('dim_date', con=connection, if_exists='append', index=False)

    except Exception as e:
        print(f"An error occurred during dim_date update: {e}");
        traceback.print_exc()
    finally:
        print("--- Incremental update for dim_date finished ---")

def incremental_update_dim_customer_scd2(engine_dwh, current_etl_datetime, last_etl_run_datetime):
    print("\n--- Starting incremental update for dim_customer (SCD Type 2) ---")

    def map_customer_record(archive_row):
        return {
            'original_customer_id': archive_row['original_customer_id'],
            'customer_name': f"{archive_row.get('first_name', '')} {archive_row.get('last_name', '')}".strip(),
            'email': archive_row.get('email'), 'phone_number': archive_row.get('phone_number'),
            'address': archive_row.get('address'), 'city': archive_row.get('city'),
            'state': archive_row.get('state'), 'zip_code': archive_row.get('zip_code'),
            'registration_date': archive_row.get('registration_date'),
            'dim_valid_from': pd.to_datetime(archive_row['valid_from_timestamp']),
            'dim_valid_to': FAR_FUTURE_DATETIME, 'is_current': True
        }

    try:
        with engine_dwh.begin() as connection:
            _generic_scd2_update(connection, "dim_customer", "archive_customers", "original_customer_id",
                                 "customer_key", last_etl_run_datetime, current_etl_datetime, map_customer_record)
    except Exception as e:
        print(f"Error during dim_customer update: {e}");
        traceback.print_exc();
        raise
    finally:
        print("--- Incremental update for dim_customer finished ---")


def incremental_update_dim_vehicle_scd2(engine_dwh, current_etl_datetime, last_etl_run_datetime):
    print("\n--- Starting incremental update for dim_vehicle (SCD Type 2) ---")

    def map_vehicle_record(archive_row):
        return {
            'original_vehicle_id': archive_row['original_vehicle_id'], 'VIN': archive_row.get('VIN'),
            'make': archive_row.get('make'), 'model': archive_row.get('model'),
            'year': archive_row.get('year'), 'fuel_type': archive_row.get('fuel_type'),
            'transmission_type': archive_row.get('transmission_type'),
            'dim_valid_from': pd.to_datetime(archive_row['valid_from_timestamp']),
            'dim_valid_to': FAR_FUTURE_DATETIME, 'is_current': True
        }

    try:
        with engine_dwh.begin() as connection:
            _generic_scd2_update(connection, "dim_vehicle", "archive_vehicles", "original_vehicle_id", "vehicle_key",
                                 last_etl_run_datetime, current_etl_datetime, map_vehicle_record)
    except Exception as e:
        print(f"Error during dim_vehicle update: {e}");
        traceback.print_exc();
        raise
    finally:
        print("--- Incremental update for dim_vehicle finished ---")


def incremental_update_dim_employee_scd2(engine_dwh, current_etl_datetime, last_etl_run_datetime):
    print("\n--- Starting incremental update for dim_employee (SCD Type 2) ---")

    def map_employee_record(archive_row):
        return {
            'original_employee_id': archive_row['original_employee_id'],
            'employee_name': f"{archive_row.get('first_name', '')} {archive_row.get('last_name', '')}".strip(),
            'role': archive_row.get('role'), 'hire_date': archive_row.get('hire_date'),
            'dim_valid_from': pd.to_datetime(archive_row['valid_from_timestamp']),
            'dim_valid_to': FAR_FUTURE_DATETIME, 'is_current': True
        }

    try:
        with engine_dwh.begin() as connection:
            _generic_scd2_update(connection, "dim_employee", "archive_employees", "original_employee_id",
                                 "employee_key", last_etl_run_datetime, current_etl_datetime, map_employee_record)
    except Exception as e:
        print(f"Error during dim_employee update: {e}");
        traceback.print_exc();
        raise
    finally:
        print("--- Incremental update for dim_employee finished ---")


def incremental_update_dim_part_scd2(engine_dwh, current_etl_datetime, last_etl_run_datetime):
    print("\n--- Starting incremental update for dim_part (SCD Type 2) ---")

    def map_part_record(archive_row):
        return {
            'original_part_id': archive_row['original_part_id'],
            'part_name': archive_row.get('part_name'), 'part_number': archive_row.get('part_number'),
            'category': archive_row.get('category'),
            'dim_valid_from': pd.to_datetime(archive_row['valid_from_timestamp']),
            'dim_valid_to': FAR_FUTURE_DATETIME, 'is_current': True
        }

    try:
        with engine_dwh.begin() as connection:
            _generic_scd2_update(connection, "dim_part", "archive_parts_inventory", "original_part_id", "part_key",
                                 last_etl_run_datetime, current_etl_datetime, map_part_record)
    except Exception as e:
        print(f"Error during dim_part update: {e}");
        traceback.print_exc();
        raise
    finally:
        print("--- Incremental update for dim_part finished ---")


def incremental_update_dim_service_type(engine_dwh, current_etl_datetime, last_etl_run_datetime):
    print("\n--- Starting incremental update for dim_service_type ---")
    archive_table = "archive_service_appointments"
    dim_table = "dim_service_type"
    try:
        with engine_dwh.begin() as connection:
            query_new_types = text(f"""
                SELECT DISTINCT service_type
                FROM {archive_table}
                WHERE service_type IS NOT NULL AND TRIM(service_type) != ''
                AND valid_to_timestamp = :far_future
                AND valid_from_timestamp > :last_run
                AND valid_from_timestamp <= :current_run
            """)
            new_archive_service_types_df = pd.read_sql_query(
                query_new_types, connection,
                params={'far_future': FAR_FUTURE_DATETIME, 'last_run': last_etl_run_datetime,
                        'current_run': current_etl_datetime}
            )
            if new_archive_service_types_df.empty:
                print(f"No new service types found in recent archive data to process for {dim_table}.")
                return

            new_archive_service_types_df.rename(columns={'service_type': 'service_type_name'}, inplace=True)
            new_archive_service_types_df['service_type_name'] = new_archive_service_types_df[
                'service_type_name'].str.strip()
            new_archive_service_types_df.drop_duplicates(subset=['service_type_name'], inplace=True)

            existing_dim_types_df = pd.read_sql_query(f"SELECT service_type_name FROM {dim_table}", connection)
            existing_types_set = set(existing_dim_types_df['service_type_name'].str.strip())
            types_to_insert_df = new_archive_service_types_df[
                ~new_archive_service_types_df['service_type_name'].isin(existing_types_set)]

            if not types_to_insert_df.empty:
                print(f"Inserting {len(types_to_insert_df)} new types into {dim_table}...")
                types_to_insert_df.to_sql(dim_table, con=connection, if_exists='append', index=False)
            else:
                print(f"No new unique service types to insert into {dim_table}.")

            print(f"Successfully processed changes for {dim_table}.")
    except Exception as e:
        print(f"An error occurred during {dim_table} update: {e}");
        traceback.print_exc()
    finally:
        print(f"--- Incremental update for {dim_table} finished ---")


def incremental_populate_fact_service_appointments(engine_dwh, current_etl_datetime, last_etl_run_datetime):
    print("\n--- Starting incremental population for fact_service_appointments ---")
    fact_table = "fact_service_appointments"
    archive_table = "archive_service_appointments"

    try:
        with engine_dwh.begin() as connection:
            new_appts_df = pd.read_sql_query(
                text(f"""SELECT * FROM {archive_table}
                         WHERE valid_from_timestamp >= :last_run AND valid_from_timestamp <= :current_run AND valid_to_timestamp = :far_future"""),
                connection, params={'last_run': last_etl_run_datetime, 'current_run': current_etl_datetime,
                                    'far_future': FAR_FUTURE_DATETIME}
            )
            if new_appts_df.empty:
                print("No new service appointments to add to the fact table.");
                return

            existing_ids = pd.read_sql_query(f"SELECT original_appointment_id FROM {fact_table}", connection)[
                'original_appointment_id'].tolist()
            new_appts_df = new_appts_df[~new_appts_df['original_appointment_id'].isin(existing_ids)]
            if new_appts_df.empty:
                print("All new appointments from archive are already in the fact table.");
                return

            print(f"Found {len(new_appts_df)} new service appointments to process.")

            dim_date_df = pd.read_sql_query("SELECT date_key, full_date, hour FROM dim_date", connection)
            dim_customer_df = pd.read_sql_query(
                "SELECT customer_key, original_customer_id FROM dim_customer WHERE is_current = TRUE", connection)
            dim_vehicle_df = pd.read_sql_query(
                "SELECT vehicle_key, original_vehicle_id FROM dim_vehicle WHERE is_current = TRUE", connection)
            dim_employee_df = pd.read_sql_query(
                "SELECT employee_key, original_employee_id FROM dim_employee WHERE is_current = TRUE", connection)
            dim_service_type_df = pd.read_sql_query("SELECT service_type_key, service_type_name FROM dim_service_type",
                                                    connection)

            for df, col in [(new_appts_df, 'customer_id'), (dim_customer_df, 'original_customer_id'),
                            (new_appts_df, 'vehicle_id'), (dim_vehicle_df, 'original_vehicle_id'),
                            (new_appts_df, 'technician_id'), (dim_employee_df, 'original_employee_id')]:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

            new_appts_df['appointment_date_dt'] = pd.to_datetime(new_appts_df['appointment_date']).dt.date

            def get_hour(time_val):
                if pd.isna(time_val): return 0
                if isinstance(time_val, str):
                    try:
                        return datetime.strptime(time_val, '%H:%M:%S').hour
                    except (ValueError, TypeError):
                        return 0
                if isinstance(time_val, timedelta): return int(time_val.total_seconds() // 3600)
                return 0

            new_appts_df['join_hour'] = new_appts_df['appointment_time'].apply(get_hour)
            new_appts_df['service_type_join'] = new_appts_df['service_type'].str.strip()
            dim_service_type_df['service_type_name'] = dim_service_type_df['service_type_name'].str.strip()
            dim_date_df['full_date'] = pd.to_datetime(dim_date_df['full_date']).dt.date

            merged_df = new_appts_df
            merged_df = merged_df.merge(dim_date_df, left_on=['appointment_date_dt', 'join_hour'],
                                        right_on=['full_date', 'hour'], how='left')
            merged_df = merged_df.merge(dim_customer_df, left_on='customer_id', right_on='original_customer_id',
                                        how='left')
            merged_df = merged_df.merge(dim_vehicle_df, left_on='vehicle_id', right_on='original_vehicle_id',
                                        how='left')
            merged_df = merged_df.merge(dim_employee_df, left_on='technician_id', right_on='original_employee_id',
                                        how='left')
            merged_df = merged_df.merge(dim_service_type_df, left_on='service_type_join', right_on='service_type_name',
                                        how='left')

            fact_df = merged_df.copy()
            essential_keys = ['date_key', 'customer_key', 'vehicle_key', 'service_type_key']

            fact_df.dropna(subset=essential_keys, inplace=True)

            if len(fact_df) < len(new_appts_df):
                print(
                    f"WARNING: Dropped {len(new_appts_df) - len(fact_df)} fact records due to failed dimension lookups.")

            if not fact_df.empty:
                final_cols = ['original_appointment_id', 'date_key', 'customer_key', 'vehicle_key', 'employee_key',
                              'service_type_key', 'total_labor_hours', 'total_parts_cost', 'total_service_cost']
                fact_df_to_load = fact_df[final_cols]
                print(f"Inserting {len(fact_df_to_load)} new records into {fact_table}...")
                fact_df_to_load.to_sql(fact_table, con=connection, if_exists='append', index=False, chunksize=1000)
            else:
                print("No new valid fact records to insert after processing.")
    except Exception as e:
        print(f"An error occurred during incremental population of {fact_table}: {e}")
        traceback.print_exc()
    finally:
        print(f"--- Incremental population for {fact_table} finished ---")


def incremental_populate_fact_service_parts_usage(engine_dwh, current_etl_datetime, last_etl_run_datetime):
    print("\n--- Starting incremental population for fact_service_parts_usage ---")
    fact_table = "fact_service_parts_usage"
    archive_details_table = "archive_service_details"
    archive_appointments_table = "archive_service_appointments"

    try:
        with engine_dwh.begin() as connection:
            new_details_df = pd.read_sql_query(
                text(f"""SELECT * FROM {archive_details_table}
                         WHERE valid_from_timestamp >= :last_run AND valid_from_timestamp <= :current_run AND valid_to_timestamp = :far_future AND part_id IS NOT NULL"""),
                connection, params={'last_run': last_etl_run_datetime, 'current_run': current_etl_datetime,
                                    'far_future': FAR_FUTURE_DATETIME}
            )
            if new_details_df.empty:
                print("No new service details with parts usage to add to the fact table.");
                return

            existing_ids = pd.read_sql_query(f"SELECT original_service_detail_id FROM {fact_table}", connection)[
                'original_service_detail_id'].tolist()
            new_details_df = new_details_df[~new_details_df['original_service_detail_id'].isin(existing_ids)]
            if new_details_df.empty:
                print("All new service details from archive are already in the fact table.");
                return

            print(f"Found {len(new_details_df)} new service detail records with parts usage to process.")

            all_appts_df = pd.read_sql_query(
                f"SELECT original_appointment_id, vehicle_id, appointment_date, appointment_time FROM {archive_appointments_table} WHERE valid_to_timestamp = '{FAR_FUTURE_DATETIME}'",
                connection)
            dim_date_df = pd.read_sql_query("SELECT date_key, full_date, hour FROM dim_date", connection)
            dim_vehicle_df = pd.read_sql_query(
                "SELECT vehicle_key, original_vehicle_id FROM dim_vehicle WHERE is_current = TRUE", connection)
            dim_part_df = pd.read_sql_query("SELECT part_key, original_part_id FROM dim_part WHERE is_current = TRUE",
                                            connection)

            for df, col in [(new_details_df, 'appointment_id'), (all_appts_df, 'original_appointment_id'),
                            (new_details_df, 'part_id'), (dim_part_df, 'original_part_id'),
                            (all_appts_df, 'vehicle_id'), (dim_vehicle_df, 'original_vehicle_id')]:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

            merged_df = new_details_df.merge(all_appts_df, left_on='appointment_id', right_on='original_appointment_id',
                                             how='left')

            merged_df['appointment_date_dt'] = pd.to_datetime(merged_df['appointment_date']).dt.date

            def get_hour(time_val):
                if pd.isna(time_val): return 0
                if isinstance(time_val, str):
                    try:
                        return datetime.strptime(time_val, '%H:%M:%S').hour
                    except (ValueError, TypeError):
                        return 0
                if isinstance(time_val, timedelta): return int(time_val.total_seconds() // 3600)
                return 0

            merged_df['join_hour'] = merged_df['appointment_time'].apply(get_hour)
            dim_date_df['full_date'] = pd.to_datetime(dim_date_df['full_date']).dt.date

            merged_df = merged_df.merge(dim_date_df, left_on=['appointment_date_dt', 'join_hour'],
                                        right_on=['full_date', 'hour'], how='left')
            merged_df = merged_df.merge(dim_vehicle_df, left_on='vehicle_id', right_on='original_vehicle_id',
                                        how='left')
            merged_df = merged_df.merge(dim_part_df, left_on='part_id', right_on='original_part_id', how='left')

            fact_df = merged_df.copy()
            essential_keys = ['original_appointment_id', 'date_key', 'vehicle_key', 'part_key']
            fact_df.dropna(subset=essential_keys, inplace=True)

            if len(fact_df) < len(new_details_df):
                print(
                    f"WARNING: Dropped {len(new_details_df) - len(fact_df)} parts usage fact records due to failed dimension lookups.")

            if not fact_df.empty:
                final_cols = ['original_service_detail_id', 'original_appointment_id', 'date_key', 'vehicle_key',
                              'part_key',
                              'quantity_used', 'unit_cost_at_time_of_service']
                fact_df_to_load = fact_df[final_cols].copy()
                fact_df_to_load['total_cost_for_part_line'] = (
                            pd.to_numeric(fact_df_to_load['quantity_used'], errors='coerce').fillna(0) * pd.to_numeric(
                        fact_df_to_load['unit_cost_at_time_of_service'], errors='coerce').fillna(0)).round(2)

                print(f"Inserting {len(fact_df_to_load)} new records into {fact_table}...")
                fact_df_to_load.to_sql(fact_table, con=connection, if_exists='append', index=False, chunksize=1000)
            else:
                print("No new valid parts usage fact records to insert after processing.")
    except Exception as e:
        print(f"An error occurred during incremental population of {fact_table}: {e}")
        traceback.print_exc()
    finally:
        print(f"--- Incremental population for {fact_table} finished ---")

def main_incremental_etl():
    global LAST_STAR_SCHEMA_ETL_RUN_DATETIME

    print("====== Starting Incremental ETL Process ======")
    current_etl_run_time = datetime.now()

    if LAST_STAR_SCHEMA_ETL_RUN_DATETIME is None:
        temp_dwh_engine = get_db_engine(DWH_DB_CONFIG, "DWH for timestamp check")
        if temp_dwh_engine:
            try:
                with temp_dwh_engine.connect() as conn:
                    ts_query = """
                               SELECT GREATEST(
                                              IFNULL((SELECT MAX(load_timestamp) FROM archive_customers), '1900-01-01'),
                                              IFNULL((SELECT MAX(load_timestamp) FROM archive_service_appointments), \
                                                     '1900-01-01'),
                                              IFNULL((SELECT MAX(load_timestamp) FROM archive_vehicles), '1900-01-01')
                                      ) as max_ts;
                               """
                    result = conn.execute(text(ts_query)).scalar()
                    if result:
                        LAST_STAR_SCHEMA_ETL_RUN_DATETIME = pd.to_datetime(result)
                        print(f"Found last load timestamp in archive: {LAST_STAR_SCHEMA_ETL_RUN_DATETIME}")
                    else:
                        LAST_STAR_SCHEMA_ETL_RUN_DATETIME = datetime(1900, 1, 1)
                        print(f"Archive is empty, using default old date: {LAST_STAR_SCHEMA_ETL_RUN_DATETIME}")
            except Exception as e:
                print(f"Could not determine last run time, using default. Error: {e}")
                LAST_STAR_SCHEMA_ETL_RUN_DATETIME = datetime(1900, 1, 1)
            finally:
                if temp_dwh_engine: temp_dwh_engine.dispose()
        else:
            LAST_STAR_SCHEMA_ETL_RUN_DATETIME = datetime(1900, 1, 1)
            print("Could not connect to DWH for timestamp check, using default old date.")

    print("\n" + "=" * 20 + " DEBUG INFO " + "=" * 20)
    print(
        f"DEBUG Main: last_run_datetime  = {LAST_STAR_SCHEMA_ETL_RUN_DATETIME} (Type: {type(LAST_STAR_SCHEMA_ETL_RUN_DATETIME)})")
    print(f"DEBUG Main: current_run_time = {current_etl_run_time} (Type: {type(current_etl_run_time)})")
    print("=" * 54 + "\n")

    source_engine = get_db_engine(SOURCE_DB_CONFIG, "Source OLTP")
    dwh_engine = get_db_engine(DWH_DB_CONFIG, "Data Warehouse")

    if not source_engine or not dwh_engine:
        print("Database engine(s) not initialized. Aborting incremental ETL.")
        return

    print("\n--- Running Incremental Archive Load Phase ---")
    customer_archive_config = {"source_table_name": "Customers", "archive_table_name": "archive_customers",
                               "source_pk_cols": ["customer_id"], "archive_original_id_col": "original_customer_id",
                               "source_to_archive_col_map": {"customer_id": "original_customer_id",
                                                             "first_name": "first_name", "last_name": "last_name",
                                                             "email": "email", "phone_number": "phone_number",
                                                             "address": "address", "city": "city", "state": "state",
                                                             "zip_code": "zip_code",
                                                             "registration_date": "registration_date",
                                                             "last_updated_timestamp": "source_last_updated_timestamp"},
                               "cols_to_compare_for_changes": ["first_name", "last_name", "email", "phone_number",
                                                               "address", "city", "state", "zip_code",
                                                               "registration_date"]}
    incremental_load_archive_table(source_engine, dwh_engine, **customer_archive_config,
                                   current_etl_datetime=current_etl_run_time)
    appt_archive_config = {"source_table_name": "ServiceAppointments",
                           "archive_table_name": "archive_service_appointments", "source_pk_cols": ["appointment_id"],
                           "archive_original_id_col": "original_appointment_id",
                           "source_to_archive_col_map": {"appointment_id": "original_appointment_id",
                                                         "vehicle_id": "vehicle_id", "customer_id": "customer_id",
                                                         "appointment_date": "appointment_date",
                                                         "appointment_time": "appointment_time",
                                                         "service_type": "service_type", "status": "status",
                                                         "technician_id": "technician_id",
                                                         "total_labor_hours": "total_labor_hours",
                                                         "total_parts_cost": "total_parts_cost",
                                                         "total_service_cost": "total_service_cost", "notes": "notes",
                                                         "created_at": "source_created_at",
                                                         "last_updated_timestamp": "source_last_updated_timestamp"},
                           "cols_to_compare_for_changes": ["vehicle_id", "customer_id", "appointment_date",
                                                           "appointment_time", "service_type", "status",
                                                           "technician_id", "total_labor_hours", "total_parts_cost",
                                                           "total_service_cost"]}
    incremental_load_archive_table(source_engine, dwh_engine, **appt_archive_config,
                                   current_etl_datetime=current_etl_run_time)
    vehicle_archive_config = {"source_table_name": "Vehicles", "archive_table_name": "archive_vehicles",
                              "source_pk_cols": ["vehicle_id"], "archive_original_id_col": "original_vehicle_id",
                              "source_to_archive_col_map": {"vehicle_id": "original_vehicle_id",
                                                            "customer_id": "customer_id", "VIN": "VIN", "make": "make",
                                                            "model": "model", "year": "year", "mileage": "mileage",
                                                            "fuel_type": "fuel_type",
                                                            "transmission_type": "transmission_type",
                                                            "last_updated_timestamp": "source_last_updated_timestamp"},
                              "cols_to_compare_for_changes": ["customer_id", "VIN", "make", "model", "year", "mileage",
                                                              "fuel_type", "transmission_type"]}
    incremental_load_archive_table(source_engine, dwh_engine, **vehicle_archive_config,
                                   current_etl_datetime=current_etl_run_time)
    employee_archive_config = {"source_table_name": "Employees", "archive_table_name": "archive_employees",
                               "source_pk_cols": ["employee_id"], "archive_original_id_col": "original_employee_id",
                               "source_to_archive_col_map": {"employee_id": "original_employee_id",
                                                             "first_name": "first_name", "last_name": "last_name",
                                                             "role": "role", "hire_date": "hire_date",
                                                             "last_updated_timestamp": "source_last_updated_timestamp"},
                               "cols_to_compare_for_changes": ["first_name", "last_name", "role", "hire_date"]}
    incremental_load_archive_table(source_engine, dwh_engine, **employee_archive_config,
                                   current_etl_datetime=current_etl_run_time)
    parts_archive_config = {"source_table_name": "PartsInventory", "archive_table_name": "archive_parts_inventory",
                            "source_pk_cols": ["part_id"], "archive_original_id_col": "original_part_id",
                            "source_to_archive_col_map": {"part_id": "original_part_id", "part_name": "part_name",
                                                          "part_number": "part_number", "category": "category",
                                                          "current_stock_quantity": "current_stock_quantity",
                                                          "unit_cost": "unit_cost",
                                                          "last_updated_timestamp": "source_last_updated_timestamp"},
                            "cols_to_compare_for_changes": ["part_name", "part_number", "category",
                                                            "current_stock_quantity", "unit_cost"]}
    incremental_load_archive_table(source_engine, dwh_engine, **parts_archive_config,
                                   current_etl_datetime=current_etl_run_time)
    service_details_archive_config = {"source_table_name": "ServiceDetails",
                                      "archive_table_name": "archive_service_details",
                                      "source_pk_cols": ["service_detail_id"],
                                      "archive_original_id_col": "original_service_detail_id",
                                      "source_to_archive_col_map": {"service_detail_id": "original_service_detail_id",
                                                                    "appointment_id": "appointment_id",
                                                                    "task_description": "task_description",
                                                                    "labor_hours": "labor_hours", "part_id": "part_id",
                                                                    "quantity_used": "quantity_used",
                                                                    "unit_cost_at_time_of_service": "unit_cost_at_time_of_service",
                                                                    "created_at": "source_created_at",
                                                                    "last_updated_timestamp": "source_last_updated_timestamp"},
                                      "cols_to_compare_for_changes": ["appointment_id", "task_description",
                                                                      "labor_hours", "part_id", "quantity_used",
                                                                      "unit_cost_at_time_of_service"]}
    incremental_load_archive_table(source_engine, dwh_engine, **service_details_archive_config,
                                   current_etl_datetime=current_etl_run_time)

    print("\n--- Incremental Archive Load Phase Complete ---")

    print("\n--- Starting Incremental Star Schema Population (Dimensions) ---")
    incremental_update_dim_date(dwh_engine, current_etl_run_time, LAST_STAR_SCHEMA_ETL_RUN_DATETIME)
    incremental_update_dim_customer_scd2(dwh_engine, current_etl_run_time, LAST_STAR_SCHEMA_ETL_RUN_DATETIME)
    incremental_update_dim_vehicle_scd2(dwh_engine, current_etl_run_time, LAST_STAR_SCHEMA_ETL_RUN_DATETIME)
    incremental_update_dim_employee_scd2(dwh_engine, current_etl_run_time, LAST_STAR_SCHEMA_ETL_RUN_DATETIME)
    incremental_update_dim_part_scd2(dwh_engine, current_etl_run_time, LAST_STAR_SCHEMA_ETL_RUN_DATETIME)
    incremental_update_dim_service_type(dwh_engine, current_etl_run_time, LAST_STAR_SCHEMA_ETL_RUN_DATETIME)

    print("\n--- Dimension Population Complete. Forcing connection reset before populating facts. ---")
    dwh_engine.dispose()
    dwh_engine = get_db_engine(DWH_DB_CONFIG, "Data Warehouse (Reconnected for Facts)")
    if not dwh_engine:
        print("Could not reconnect to DWH for fact population. Aborting.")
        return

    print("\n--- Starting Incremental Star Schema Population (Facts) ---")
    incremental_populate_fact_service_appointments(dwh_engine, current_etl_run_time, LAST_STAR_SCHEMA_ETL_RUN_DATETIME)
    incremental_populate_fact_service_parts_usage(dwh_engine, current_etl_run_time, LAST_STAR_SCHEMA_ETL_RUN_DATETIME)

    print(f"\nUpdating LAST_STAR_SCHEMA_ETL_RUN_DATETIME to: {current_etl_run_time} for next run.")
    LAST_STAR_SCHEMA_ETL_RUN_DATETIME = current_etl_run_time

    print("\n====== Incremental ETL Process Finished ======")


if __name__ == "__main__":
    time.sleep(10)
    main_incremental_etl()
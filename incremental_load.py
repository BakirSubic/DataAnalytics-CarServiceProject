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

    print(f"Reading all data from source table: {source_table_name}...")
    source_df_query = f"SELECT * FROM {source_table_name}"
    try:
        source_df = pd.read_sql_query(source_df_query, engine_source)
    except Exception as e:
        print(f"Error reading from source table {source_table_name}: {e}");
        return

    if source_table_name == "ServiceAppointments" and 'appointment_time' in source_df.columns:
        print(f"Formatting 'appointment_time' for {source_table_name}...")
        source_df['appointment_time'] = source_df['appointment_time'].apply(format_time)

    if source_df.empty:
        print(f"No data in source table {source_table_name}. Checking for deletions in archive...")
    else:
        source_df.dropna(subset=source_pk_cols, inplace=True)
        if source_df.empty: print(
            f"No data with non-null PKs in source table {source_table_name}. Checking for deletions...")

    print(f"Reading current versions from archive table: {archive_table_name}...")
    current_archive_query = f"SELECT * FROM {archive_table_name} WHERE valid_to_timestamp = :far_future"
    try:
        with engine_dwh.connect() as conn_read:
            current_archive_df = pd.read_sql_query(text(current_archive_query), conn_read,
                                                   params={'far_future': FAR_FUTURE_DATETIME})
    except Exception as e:
        print(f"Error reading current versions from archive table {archive_table_name}: {e}");
        return

    prepared_source_data = []
    if not source_df.empty:
        for _, source_row in source_df.iterrows():
            archive_equivalent_row = {a_col: source_row.get(s_col) for s_col, a_col in
                                      source_to_archive_col_map.items()}
            prepared_source_data.append(archive_equivalent_row)
        source_df_renamed = pd.DataFrame(prepared_source_data)
    else:
        source_df_renamed = pd.DataFrame(columns=list(source_to_archive_col_map.values()))

    merged_df = pd.DataFrame()
    archive_was_empty = current_archive_df.empty

    if not source_df_renamed.empty and not archive_was_empty:
        merged_df = pd.merge(source_df_renamed, current_archive_df, on=[archive_original_id_col], how='outer',
                             suffixes=('_source', '_archive'), indicator=True)
    elif not source_df_renamed.empty:
        merged_df = source_df_renamed.copy();
        merged_df['_merge'] = 'left_only'
    elif not archive_was_empty:
        merged_df = current_archive_df.copy();
        merged_df['_merge'] = 'right_only'
        if archive_original_id_col not in merged_df.columns and f"{archive_original_id_col}_archive" in merged_df.columns:
            merged_df[archive_original_id_col] = merged_df[f"{archive_original_id_col}_archive"]

    records_to_insert_new = []
    records_to_close_original_ids = []

    if not merged_df.empty:
        new_records_df = merged_df[merged_df['_merge'] == 'left_only']
        for _, row in new_records_df.iterrows():
            new_rec = {}
            for target_archive_col_name in source_df_renamed.columns:
                source_suffixed_col = f"{target_archive_col_name}_source"
                if not archive_was_empty and source_suffixed_col in row.index:
                    new_rec[target_archive_col_name] = row[source_suffixed_col]
                elif target_archive_col_name in row.index:
                    new_rec[target_archive_col_name] = row[target_archive_col_name]
                else:
                    new_rec[target_archive_col_name] = None
            new_rec.update({'load_timestamp': current_etl_datetime, 'valid_from_timestamp': current_etl_datetime,
                            'valid_to_timestamp': FAR_FUTURE_DATETIME})
            records_to_insert_new.append(new_rec)
        print(f"Identified {len(new_records_df)} new records for {archive_table_name}.")

        deleted_records_df = merged_df[merged_df['_merge'] == 'right_only']
        if not deleted_records_df.empty:
            records_to_close_original_ids.extend(deleted_records_df[archive_original_id_col].tolist())
            print(f"Identified {len(deleted_records_df)} records deleted from source for {archive_table_name}.")

        potential_updates_df = merged_df[merged_df['_merge'] == 'both']
        for _, row in potential_updates_df.iterrows():
            is_changed = False
            for archive_col_to_compare in cols_to_compare_for_changes:
                source_val, archive_val = row.get(f"{archive_col_to_compare}_source"), row.get(
                    f"{archive_col_to_compare}_archive")
                if pd.isna(source_val) and pd.isna(archive_val): continue
                if pd.isna(source_val) or pd.isna(archive_val): is_changed = True; break
                if isinstance(source_val, (datetime_date, datetime, datetime_time, str)) and isinstance(archive_val,
                                                                                                        (datetime_date,
                                                                                                         datetime,
                                                                                                         datetime_time,
                                                                                                         str)):
                    if str(source_val) != str(archive_val): is_changed = True; break
                elif isinstance(source_val, (int, float)) and isinstance(archive_val, (int, float)):
                    if not np.isclose(source_val, archive_val, equal_nan=True): is_changed = True; break
                elif str(source_val) != str(archive_val):
                    is_changed = True; break

            source_luts_archive_name = source_to_archive_col_map.get('last_updated_timestamp')
            if not is_changed and source_luts_archive_name:
                s_luts_s, s_luts_a = f"{source_luts_archive_name}_source", f"{source_luts_archive_name}_archive"
                if s_luts_s in row and s_luts_a in row and pd.notna(row[s_luts_s]) and pd.notna(row[s_luts_a]):
                    if pd.to_datetime(row[s_luts_s], errors='coerce') > pd.to_datetime(row[s_luts_a], errors='coerce'):
                        is_changed = True

            if is_changed:
                records_to_close_original_ids.append(row[archive_original_id_col])
                updated_rec = {target_col: (
                    row[archive_original_id_col] if target_col == archive_original_id_col else row.get(
                        f"{target_col}_source")) for target_col in source_df_renamed.columns}
                updated_rec.update(
                    {'load_timestamp': current_etl_datetime, 'valid_from_timestamp': current_etl_datetime,
                     'valid_to_timestamp': FAR_FUTURE_DATETIME})
                records_to_insert_new.append(updated_rec)
        print(
            f"Identified {len(records_to_insert_new) - len(new_records_df)} updated records for {archive_table_name}.")

    try:
        with engine_dwh.begin() as connection:
            try:
                if records_to_close_original_ids:
                    unique_ids_to_close = list(set(records_to_close_original_ids))
                    print(f"Closing out {len(unique_ids_to_close)} old/deleted records in {archive_table_name}...")
                    if unique_ids_to_close:
                        chunk_size_update = 500
                        for i in range(0, len(unique_ids_to_close), chunk_size_update):
                            id_chunk = unique_ids_to_close[i:i + chunk_size_update]
                            placeholders = ', '.join([f':id_{j}' for j in range(len(id_chunk))])
                            params_for_close = {f'id_{j}': id_val for j, id_val in enumerate(id_chunk)}
                            params_for_close.update(
                                {'closing_ts': closing_timestamp, 'far_future': FAR_FUTURE_DATETIME})
                            stmt_close = text(
                                f"UPDATE {archive_table_name} SET valid_to_timestamp = :closing_ts WHERE {archive_original_id_col} IN ({placeholders}) AND valid_to_timestamp = :far_future")
                            connection.execute(stmt_close, params_for_close)
                if records_to_insert_new:
                    print(f"Inserting {len(records_to_insert_new)} new/updated records into {archive_table_name}...")
                    insert_df = pd.DataFrame(records_to_insert_new)
                    all_archive_cols = get_table_columns(engine_dwh, archive_table_name)
                    insert_df = insert_df[[col for col in insert_df.columns if col in all_archive_cols]]
                    insert_df.to_sql(archive_table_name, con=connection, if_exists='append', index=False,
                                     chunksize=1000)

                print(f"Successfully committed changes for {archive_table_name}.")
            except Exception as e_inner:
                print(
                    f"Error during DB operations for {archive_table_name}, transaction will be rolled back: {e_inner}");
                traceback.print_exc()
                raise
    except Exception as e_outer:
        print(f"Outer error during DB operations for {archive_table_name}: {e_outer}");
        traceback.print_exc()
    print(f"--- Incremental load for archive table: {archive_table_name} finished ---")


def incremental_update_dim_customer_scd2(engine_dwh, current_etl_datetime, last_etl_run_datetime):
    print("\n--- Starting incremental update for dim_customer (SCD Type 2) ---")
    archive_table = "archive_customers"
    dim_table = "dim_customer"
    original_id_col = "original_customer_id"
    far_future_dim = FAR_FUTURE_DATETIME

    try:
        with engine_dwh.begin() as connection:
            try:
                newly_active_archive_query = text(f"""
                    SELECT * FROM {archive_table}
                    WHERE (valid_from_timestamp >= :last_run AND valid_from_timestamp < :current_run)
                       OR (valid_to_timestamp >= :last_run AND valid_to_timestamp < :current_run AND valid_to_timestamp != :far_future)
                """)

                changed_archive_df = pd.read_sql_query(
                    newly_active_archive_query, connection,
                    params={'last_run': last_etl_run_datetime, 'current_run': current_etl_datetime,
                            'far_future': FAR_FUTURE_DATETIME}
                )
                print(f"Found {len(changed_archive_df)} potentially changed/new customer versions in archive.")
                if changed_archive_df.empty:
                    print("No new customer versions from archive to process for dim_customer.")
                    return

                current_dim_df = pd.read_sql_query(
                    f"SELECT * FROM {dim_table} WHERE is_current = TRUE", connection
                )

                records_to_insert_dim = []
                ids_to_close_in_dim = {}

                for original_id, group in changed_archive_df.groupby(original_id_col):
                    group = group.sort_values(by='valid_from_timestamp')
                    current_dim_record_for_id_df = current_dim_df[current_dim_df[original_id_col] == original_id]

                    for _, archive_row in group.iterrows():
                        archive_valid_from = pd.to_datetime(archive_row['valid_from_timestamp'], errors='coerce')
                        archive_valid_to = pd.to_datetime(archive_row['valid_to_timestamp'], errors='coerce')

                        if pd.isna(archive_valid_from):
                            print(
                                f"Warning: Skipping archive customer record for original_id {original_id} due to invalid valid_from_timestamp: {archive_row['valid_from_timestamp']}")
                            continue

                        if not current_dim_record_for_id_df.empty:
                            dim_row_to_close = current_dim_record_for_id_df.iloc[0]
                            dim_valid_from_ts = pd.to_datetime(dim_row_to_close['dim_valid_from'], errors='coerce')

                            is_newer_active_version = False
                            if pd.notna(dim_valid_from_ts) and pd.notna(archive_valid_from):
                                if archive_valid_from > dim_valid_from_ts and archive_valid_to == FAR_FUTURE_DATETIME:
                                    is_newer_active_version = True
                            elif pd.notna(archive_valid_from) and archive_valid_to == FAR_FUTURE_DATETIME:
                                is_newer_active_version = True

                            is_archive_closure = archive_valid_to != FAR_FUTURE_DATETIME

                            if dim_row_to_close['is_current']:
                                if is_newer_active_version:
                                    ids_to_close_in_dim[original_id] = {
                                        'customer_key': dim_row_to_close['customer_key'],
                                        'new_dim_valid_to': archive_valid_from - timedelta(microseconds=1)
                                    }
                                    records_to_insert_dim.append({
                                        original_id_col: original_id,
                                        'customer_name': f"{archive_row.get('first_name', '')} {archive_row.get('last_name', '')}".strip(),
                                        'email': archive_row.get('email'),
                                        'phone_number': archive_row.get('phone_number'),
                                        'address': archive_row.get('address'), 'city': archive_row.get('city'),
                                        'state': archive_row.get('state'), 'zip_code': archive_row.get('zip_code'),
                                        'registration_date': archive_row.get('registration_date'),
                                        'dim_valid_from': archive_valid_from,
                                        'dim_valid_to': far_future_dim,
                                        'is_current': True
                                    })
                                    current_dim_record_for_id_df = pd.DataFrame()
                                elif is_archive_closure:
                                    ids_to_close_in_dim[original_id] = {
                                        'customer_key': dim_row_to_close['customer_key'],
                                        'new_dim_valid_to': archive_valid_to
                                    }
                                    current_dim_record_for_id_df = pd.DataFrame()
                            elif not dim_row_to_close['is_current'] and is_newer_active_version:
                                records_to_insert_dim.append({
                                    original_id_col: original_id,
                                    'customer_name': f"{archive_row.get('first_name', '')} {archive_row.get('last_name', '')}".strip(),
                                    'email': archive_row.get('email'), 'phone_number': archive_row.get('phone_number'),
                                    'address': archive_row.get('address'), 'city': archive_row.get('city'),
                                    'state': archive_row.get('state'), 'zip_code': archive_row.get('zip_code'),
                                    'registration_date': archive_row.get('registration_date'),
                                    'dim_valid_from': archive_valid_from,
                                    'dim_valid_to': far_future_dim,
                                    'is_current': True
                                })
                                current_dim_record_for_id_df = pd.DataFrame()

                        elif current_dim_record_for_id_df.empty and archive_valid_to == FAR_FUTURE_DATETIME:
                            records_to_insert_dim.append({
                                original_id_col: original_id,
                                'customer_name': f"{archive_row.get('first_name', '')} {archive_row.get('last_name', '')}".strip(),
                                'email': archive_row.get('email'), 'phone_number': archive_row.get('phone_number'),
                                'address': archive_row.get('address'), 'city': archive_row.get('city'),
                                'state': archive_row.get('state'), 'zip_code': archive_row.get('zip_code'),
                                'registration_date': archive_row.get('registration_date'),
                                'dim_valid_from': archive_valid_from,
                                'dim_valid_to': far_future_dim,
                                'is_current': True
                            })

                if records_to_insert_dim:
                    temp_insert_df = pd.DataFrame(records_to_insert_dim)
                    temp_insert_df.drop_duplicates(subset=[original_id_col, 'dim_valid_from'], keep='last',
                                                   inplace=True)
                    records_to_insert_dim = temp_insert_df.to_dict('records')

                if ids_to_close_in_dim:
                    print(f"Closing out {len(ids_to_close_in_dim)} old versions in {dim_table}...")
                    for original_id_key, details in ids_to_close_in_dim.items():
                        stmt_close_dim = text(f"""
                            UPDATE {dim_table}
                            SET is_current = FALSE, dim_valid_to = :new_dim_valid_to
                            WHERE customer_key = :customer_key AND is_current = TRUE 
                                  AND (:new_dim_valid_to >= dim_valid_from OR dim_valid_from IS NULL) 
                        """)
                        customer_key_val = int(details['customer_key'])
                        connection.execute(stmt_close_dim, {'new_dim_valid_to': details['new_dim_valid_to'],
                                                            'customer_key': customer_key_val})

                if records_to_insert_dim:
                    print(f"Inserting {len(records_to_insert_dim)} new versions into {dim_table}...")
                    insert_dim_df = pd.DataFrame(records_to_insert_dim)
                    dim_table_cols = get_table_columns(engine_dwh, dim_table)
                    insert_dim_df = insert_dim_df[[col for col in insert_dim_df.columns if col in dim_table_cols]]
                    if not insert_dim_df.empty:
                        insert_dim_df.to_sql(dim_table, con=connection, if_exists='append', index=False, chunksize=1000)

                print(f"Successfully processed changes for {dim_table}.")
            except Exception as e_inner:
                print(f"Error during DB operations for {dim_table}, transaction will be rolled back: {e_inner}");
                traceback.print_exc()
                raise
        print(f"--- Incremental update for {dim_table} finished ---")
    except Exception as e_outer:
        print(f"General error during {dim_table} update: {e_outer}");
        traceback.print_exc()


def incremental_update_dim_service_type(engine_dwh, current_etl_datetime, last_etl_run_datetime):
    print("\n--- Starting incremental update for dim_service_type ---")
    archive_table = "archive_service_appointments"
    dim_table = "dim_service_type"
    try:
        with engine_dwh.begin() as connection:
            try:
                query_new_types = text(f"""
                    SELECT DISTINCT service_type 
                    FROM {archive_table}
                    WHERE service_type IS NOT NULL AND TRIM(service_type) != ''
                    AND valid_to_timestamp = :far_future 
                    AND valid_from_timestamp >= :last_run 
                """)
                new_archive_service_types_df = pd.read_sql_query(
                    query_new_types, connection,
                    params={'far_future': FAR_FUTURE_DATETIME, 'last_run': last_etl_run_datetime}
                )
                if new_archive_service_types_df.empty:
                    print("No new service types found in recent archive data to process for dim_service_type.")
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
            except Exception as e_inner:
                print(f"Error during DB operations for {dim_table}, transaction will be rolled back: {e_inner}");
                traceback.print_exc()
                raise
        print(f"--- Incremental update for {dim_table} finished ---")
    except Exception as e_outer:
        print(f"An error occurred during {dim_table} update: {e_outer}");
        traceback.print_exc()


def incremental_update_dim_vehicle_scd2(engine_dwh, current_etl_datetime, last_etl_run_datetime):
    print("\n--- Starting incremental update for dim_vehicle (SCD Type 2) ---")
    archive_table = "archive_vehicles"
    dim_table = "dim_vehicle"
    original_id_col = "original_vehicle_id"
    far_future_dim = FAR_FUTURE_DATETIME

    try:
        with engine_dwh.begin() as connection:
            try:
                newly_active_archive_query = text(f"""
                    SELECT * FROM {archive_table}
                    WHERE (valid_from_timestamp >= :last_run AND valid_from_timestamp < :current_run)
                       OR (valid_to_timestamp >= :last_run AND valid_to_timestamp < :current_run AND valid_to_timestamp != :far_future)
                """)
                changed_archive_df = pd.read_sql_query(
                    newly_active_archive_query, connection,
                    params={'last_run': last_etl_run_datetime, 'current_run': current_etl_datetime,
                            'far_future': FAR_FUTURE_DATETIME}
                )
                print(f"Found {len(changed_archive_df)} potentially changed/new vehicle versions in archive.")
                if changed_archive_df.empty:
                    print("No new vehicle versions from archive to process for dim_vehicle.")
                    return

                current_dim_df = pd.read_sql_query(
                    f"SELECT * FROM {dim_table} WHERE is_current = TRUE", connection
                )

                records_to_insert_dim = []
                ids_to_close_in_dim = {}

                for original_id, group in changed_archive_df.groupby(original_id_col):
                    group = group.sort_values(by='valid_from_timestamp')
                    current_dim_record_for_id_df = current_dim_df[current_dim_df[original_id_col] == original_id]

                    for _, archive_row in group.iterrows():
                        archive_valid_from = pd.to_datetime(archive_row['valid_from_timestamp'], errors='coerce')
                        archive_valid_to = pd.to_datetime(archive_row['valid_to_timestamp'], errors='coerce')

                        if pd.isna(archive_valid_from):
                            print(
                                f"Warning: Skipping archive vehicle record for original_id {original_id} due to invalid valid_from_timestamp.")
                            continue

                        dim_vehicle_data = {
                            original_id_col: original_id,
                            'VIN': archive_row.get('VIN'),
                            'make': archive_row.get('make'),
                            'model': archive_row.get('model'),
                            'year': archive_row.get('year'),
                            'fuel_type': archive_row.get('fuel_type'),
                            'transmission_type': archive_row.get('transmission_type'),
                            'dim_valid_from': archive_valid_from,
                            'dim_valid_to': far_future_dim,
                            'is_current': True
                        }

                        if not current_dim_record_for_id_df.empty:
                            dim_row_to_close = current_dim_record_for_id_df.iloc[0]
                            dim_valid_from_ts = pd.to_datetime(dim_row_to_close['dim_valid_from'], errors='coerce')

                            is_newer_active_version = False
                            if pd.notna(dim_valid_from_ts) and pd.notna(archive_valid_from):
                                if archive_valid_from > dim_valid_from_ts and archive_valid_to == FAR_FUTURE_DATETIME:
                                    is_newer_active_version = True
                            elif pd.notna(archive_valid_from) and archive_valid_to == FAR_FUTURE_DATETIME:
                                is_newer_active_version = True

                            is_archive_closure = archive_valid_to != FAR_FUTURE_DATETIME

                            if dim_row_to_close['is_current']:
                                if is_newer_active_version:
                                    ids_to_close_in_dim[original_id] = {
                                        'vehicle_key': dim_row_to_close['vehicle_key'],
                                        'new_dim_valid_to': archive_valid_from - timedelta(microseconds=1)
                                    }
                                    records_to_insert_dim.append(dim_vehicle_data)
                                    current_dim_record_for_id_df = pd.DataFrame()
                                elif is_archive_closure:
                                    ids_to_close_in_dim[original_id] = {
                                        'vehicle_key': dim_row_to_close['vehicle_key'],
                                        'new_dim_valid_to': archive_valid_to
                                    }
                                    current_dim_record_for_id_df = pd.DataFrame()
                            elif not dim_row_to_close[
                                'is_current'] and is_newer_active_version:
                                records_to_insert_dim.append(dim_vehicle_data)
                                current_dim_record_for_id_df = pd.DataFrame()


                        elif current_dim_record_for_id_df.empty and archive_valid_to == FAR_FUTURE_DATETIME:
                            records_to_insert_dim.append(dim_vehicle_data)

                if records_to_insert_dim:
                    temp_insert_df = pd.DataFrame(records_to_insert_dim)
                    temp_insert_df.drop_duplicates(subset=[original_id_col, 'dim_valid_from'], keep='last',
                                                   inplace=True)
                    records_to_insert_dim = temp_insert_df.to_dict('records')

                if ids_to_close_in_dim:
                    print(f"Closing out {len(ids_to_close_in_dim)} old versions in {dim_table}...")
                    for original_id_key, details in ids_to_close_in_dim.items():
                        stmt_close_dim = text(f"""
                            UPDATE {dim_table}
                            SET is_current = FALSE, dim_valid_to = :new_dim_valid_to
                            WHERE vehicle_key = :vehicle_key AND is_current = TRUE
                                  AND (:new_dim_valid_to >= dim_valid_from OR dim_valid_from IS NULL)
                        """)
                        vehicle_key_val = int(details['vehicle_key'])
                        connection.execute(stmt_close_dim, {'new_dim_valid_to': details['new_dim_valid_to'],
                                                            'vehicle_key': vehicle_key_val})

                if records_to_insert_dim:
                    print(f"Inserting {len(records_to_insert_dim)} new versions into {dim_table}...")
                    insert_dim_df = pd.DataFrame(records_to_insert_dim)
                    dim_table_cols = get_table_columns(engine_dwh, dim_table)
                    insert_dim_df = insert_dim_df[[col for col in insert_dim_df.columns if col in dim_table_cols]]
                    if not insert_dim_df.empty:
                        insert_dim_df.to_sql(dim_table, con=connection, if_exists='append', index=False, chunksize=1000)

                print(f"Successfully processed changes for {dim_table}.")
            except Exception as e_inner:
                print(f"Error during DB operations for {dim_table}, transaction will be rolled back: {e_inner}");
                traceback.print_exc()
                raise
        print(f"--- Incremental update for {dim_table} finished ---")
    except Exception as e_outer:
        print(f"General error during {dim_table} update: {e_outer}");
        traceback.print_exc()


def incremental_update_dim_employee_scd2(engine_dwh, current_etl_datetime, last_etl_run_datetime):
    print("\n--- Starting incremental update for dim_employee (SCD Type 2) ---")
    archive_table = "archive_employees"
    dim_table = "dim_employee"
    original_id_col = "original_employee_id"
    far_future_dim = FAR_FUTURE_DATETIME

    try:
        with engine_dwh.begin() as connection:
            try:
                newly_active_archive_query = text(f"""
                    SELECT * FROM {archive_table}
                    WHERE (valid_from_timestamp >= :last_run AND valid_from_timestamp < :current_run)
                       OR (valid_to_timestamp >= :last_run AND valid_to_timestamp < :current_run AND valid_to_timestamp != :far_future)
                """)
                changed_archive_df = pd.read_sql_query(
                    newly_active_archive_query, connection,
                    params={'last_run': last_etl_run_datetime, 'current_run': current_etl_datetime,
                            'far_future': FAR_FUTURE_DATETIME}
                )
                print(f"Found {len(changed_archive_df)} potentially changed/new employee versions in archive.")
                if changed_archive_df.empty:
                    print("No new employee versions from archive to process for dim_employee.")
                    return

                current_dim_df = pd.read_sql_query(
                    f"SELECT * FROM {dim_table} WHERE is_current = TRUE", connection
                )

                records_to_insert_dim = []
                ids_to_close_in_dim = {}

                for original_id, group in changed_archive_df.groupby(original_id_col):
                    group = group.sort_values(by='valid_from_timestamp')
                    current_dim_record_for_id_df = current_dim_df[current_dim_df[original_id_col] == original_id]

                    for _, archive_row in group.iterrows():
                        archive_valid_from = pd.to_datetime(archive_row['valid_from_timestamp'], errors='coerce')
                        archive_valid_to = pd.to_datetime(archive_row['valid_to_timestamp'], errors='coerce')

                        if pd.isna(archive_valid_from):
                            print(
                                f"Warning: Skipping archive employee record for original_id {original_id} due to invalid valid_from_timestamp.")
                            continue

                        dim_employee_data = {
                            original_id_col: original_id,
                            'employee_name': f"{archive_row.get('first_name', '')} {archive_row.get('last_name', '')}".strip(),
                            'role': archive_row.get('role'),
                            'hire_date': archive_row.get('hire_date'),
                            'dim_valid_from': archive_valid_from,
                            'dim_valid_to': far_future_dim,
                            'is_current': True
                        }

                        if not current_dim_record_for_id_df.empty:
                            dim_row_to_close = current_dim_record_for_id_df.iloc[0]
                            dim_valid_from_ts = pd.to_datetime(dim_row_to_close['dim_valid_from'], errors='coerce')

                            is_newer_active_version = False
                            if pd.notna(dim_valid_from_ts) and pd.notna(archive_valid_from):
                                if archive_valid_from > dim_valid_from_ts and archive_valid_to == FAR_FUTURE_DATETIME:
                                    is_newer_active_version = True
                            elif pd.notna(archive_valid_from) and archive_valid_to == FAR_FUTURE_DATETIME:
                                is_newer_active_version = True

                            is_archive_closure = archive_valid_to != FAR_FUTURE_DATETIME

                            if dim_row_to_close['is_current']:
                                if is_newer_active_version:
                                    ids_to_close_in_dim[original_id] = {
                                        'employee_key': dim_row_to_close['employee_key'],
                                        'new_dim_valid_to': archive_valid_from - timedelta(microseconds=1)
                                    }
                                    records_to_insert_dim.append(dim_employee_data)
                                    current_dim_record_for_id_df = pd.DataFrame()
                                elif is_archive_closure:
                                    ids_to_close_in_dim[original_id] = {
                                        'employee_key': dim_row_to_close['employee_key'],
                                        'new_dim_valid_to': archive_valid_to
                                    }
                                    current_dim_record_for_id_df = pd.DataFrame()
                            elif not dim_row_to_close['is_current'] and is_newer_active_version:
                                records_to_insert_dim.append(dim_employee_data)
                                current_dim_record_for_id_df = pd.DataFrame()

                        elif current_dim_record_for_id_df.empty and archive_valid_to == FAR_FUTURE_DATETIME:
                            records_to_insert_dim.append(dim_employee_data)

                if records_to_insert_dim:
                    temp_insert_df = pd.DataFrame(records_to_insert_dim)
                    temp_insert_df.drop_duplicates(subset=[original_id_col, 'dim_valid_from'], keep='last',
                                                   inplace=True)
                    records_to_insert_dim = temp_insert_df.to_dict('records')

                if ids_to_close_in_dim:
                    print(f"Closing out {len(ids_to_close_in_dim)} old versions in {dim_table}...")
                    for original_id_key, details in ids_to_close_in_dim.items():
                        stmt_close_dim = text(f"""
                            UPDATE {dim_table}
                            SET is_current = FALSE, dim_valid_to = :new_dim_valid_to
                            WHERE employee_key = :employee_key AND is_current = TRUE
                                  AND (:new_dim_valid_to >= dim_valid_from OR dim_valid_from IS NULL)
                        """)
                        employee_key_val = int(details['employee_key'])
                        connection.execute(stmt_close_dim, {'new_dim_valid_to': details['new_dim_valid_to'],
                                                            'employee_key': employee_key_val})

                if records_to_insert_dim:
                    print(f"Inserting {len(records_to_insert_dim)} new versions into {dim_table}...")
                    insert_dim_df = pd.DataFrame(records_to_insert_dim)
                    dim_table_cols = get_table_columns(engine_dwh, dim_table)
                    insert_dim_df = insert_dim_df[[col for col in insert_dim_df.columns if col in dim_table_cols]]
                    if not insert_dim_df.empty:
                        insert_dim_df.to_sql(dim_table, con=connection, if_exists='append', index=False, chunksize=1000)

                print(f"Successfully processed changes for {dim_table}.")
            except Exception as e_inner:
                print(f"Error during DB operations for {dim_table}, transaction will be rolled back: {e_inner}");
                traceback.print_exc()
                raise
        print(f"--- Incremental update for {dim_table} finished ---")
    except Exception as e_outer:
        print(f"General error during {dim_table} update: {e_outer}");
        traceback.print_exc()


def main_incremental_etl():
    global LAST_STAR_SCHEMA_ETL_RUN_DATETIME

    print("====== Starting Incremental ETL Process ======")
    current_etl_run_time = datetime.now()

    if LAST_STAR_SCHEMA_ETL_RUN_DATETIME is None:
        LAST_STAR_SCHEMA_ETL_RUN_DATETIME = datetime(1900, 1, 1)
        print(
            f"First incremental run (or LAST_STAR_SCHEMA_ETL_RUN_DATETIME not set), using: {LAST_STAR_SCHEMA_ETL_RUN_DATETIME} to catch all archive changes for dimensions.")

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
                                                           "total_service_cost", "notes"]}
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

    print("\n--- Starting Incremental Star Schema Population ---")

    incremental_update_dim_customer_scd2(dwh_engine, current_etl_run_time, LAST_STAR_SCHEMA_ETL_RUN_DATETIME)
    incremental_update_dim_service_type(dwh_engine, current_etl_run_time, LAST_STAR_SCHEMA_ETL_RUN_DATETIME)
    incremental_update_dim_vehicle_scd2(dwh_engine, current_etl_run_time, LAST_STAR_SCHEMA_ETL_RUN_DATETIME)
    incremental_update_dim_employee_scd2(dwh_engine, current_etl_run_time, LAST_STAR_SCHEMA_ETL_RUN_DATETIME)


    print("\n--- Placeholder for Incremental Fact Table Population ---")
    print("Fact table incremental load needs to be implemented.")

    print(f"\nUpdating LAST_STAR_SCHEMA_ETL_RUN_DATETIME to: {current_etl_run_time} for next run.")
    LAST_STAR_SCHEMA_ETL_RUN_DATETIME = current_etl_run_time

    print("\n====== Incremental ETL Process Finished ======")


if __name__ == "__main__":
    main_incremental_etl()

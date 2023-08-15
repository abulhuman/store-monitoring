import pandas as pd
import numpy as np
import psycopg2
from tqdm import tqdm


def is_table_populated(connection, table_name):
    cursor = connection.cursor()
    check_query = f'SELECT ({table_name}) FROM "_DataProgress" WHERE id = 1'
    cursor.execute(check_query)
    populated = cursor.fetchone()
    if populated is None:
        skip_insertion = False
    else:
        skip_insertion = populated[0]
    cursor.close()
    print(f'is_table_populated({table_name}) -> {skip_insertion}')
    return skip_insertion

def process_business_hours():
    print('Reading "business_hours.csv"...')
    business_hours = pd.read_csv('data_source/business_hours.csv')
    business_hours.fillna({'start_time_local': '00:00:00', 'end_time_local': '23:59:59'}, inplace=True)
    business_hours['start_time_local'] = pd.to_datetime(business_hours['start_time_local'], format='%H:%M:%S').dt.time
    business_hours['end_time_local'] = pd.to_datetime(business_hours['end_time_local'], format='%H:%M:%S').dt.time
    print(f'"Business Hours" Row Count = {len(business_hours)}')

    return business_hours


def insert_business_hours(business_hours_connection, processed_business_hours_dataframe):
    cursor = business_hours_connection.cursor()
    skip_insertion = is_table_populated(business_hours_connection, 'business_hours_populated')
    if skip_insertion:
        return
    def repair_business_hours_record(repair_business_hours_connection, business_hours_row):
        repair_cursor = repair_business_hours_connection.cursor()
        timezone_repair_data = {'store_id': business_hours_row['store_id'], 'timezone': 'America/Chicago'}
        repair_cursor.execute(
            f'INSERT INTO "StoreTimezones" (store_id, timezone) VALUES ({timezone_repair_data["store_id"]}, \'{timezone_repair_data["timezone"]}\')')
        repair_business_hours_connection.commit()
        repair_cursor.execute(
            f'INSERT INTO "StoreBusinessHours" (store_id, day_of_week, start_time_local, end_time_local) VALUES ({business_hours_row["store_id"]}, \'{business_hours_row["day_of_week"]}\', \'{business_hours_row["start_time_local"]}\', \'{business_hours_row["end_time_local"]}\')')
        repair_business_hours_connection.commit()
        repair_cursor.close()

    broken_store_count = 0
    broken_stores = []
    for row in tqdm(np.array(processed_business_hours_dataframe), smoothing=0.9):
        insert_query = 'INSERT INTO "StoreBusinessHours" (store_id, day_of_week, start_time_local, end_time_local) VALUES (%s, %s, %s, %s)'
        try:
            cursor.execute(insert_query, row)
            business_hours_connection.commit()
        except Exception:
            business_hours_connection.rollback()
            dict_row = {'store_id': row[0], 'day_of_week': row[1], 'start_time_local': row[2], 'end_time_local': row[3]}
            broken_stores.append(dict_row)
            broken_store_count += 1

    for broken_store in tqdm(broken_stores, smoothing=0.9):
        repair_business_hours_record(business_hours_connection, broken_store)

    print(
        f'Inserted all business hours ({len(processed_business_hours_dataframe)}) and fixed {broken_store_count} broken(timezone-less) stores')

    cursor.execute('UPDATE "_DataProgress" SET business_hours_populated = %s WHERE id = %s',
                   (True, 1))
    business_hours_connection.commit()
    cursor.close()


def process_store_status():
    print('Reading "store_status.csv"...')
    store_status = pd.read_csv('data_source/store_status.csv')
    # store_status['timestamp_utc'] = pd.to_datetime(store_status['timestamp_utc'], format='mixed')
    print(f'"Store Status" Row Count = {len(store_status)}')

    return store_status


def insert_store_status(store_status_connection, processed_store_status_dataframe):
    cursor = store_status_connection.cursor()
    skip_insertion = is_table_populated(store_status_connection, 'store_status_populated')
    if skip_insertion:
        return
    def repair_store_status_record(repair_store_status_connection, store_status_row):
        repair_cursor = repair_store_status_connection.cursor()
        timezone_repair_data = {'store_id': store_status_row['store_id'], 'timezone': 'America/Chicago'}
        repair_cursor.execute(
            f'INSERT INTO "StoreTimezones" (store_id, timezone) VALUES ({timezone_repair_data["store_id"]}, \'{timezone_repair_data["timezone"]}\')')
        repair_store_status_connection.commit()
        for day in range(7):
            try:
                repair_cursor.execute(
                    f'INSERT INTO "StoreBusinessHours" (store_id, day_of_week, start_time_local, end_time_local) VALUES ({store_status_row["store_id"]}, {day}, \'00:00:00\', \'23:59:59\')')
                repair_store_status_connection.commit()
            except Exception:
                repair_store_status_connection.rollback()
                continue
        repair_cursor.execute(
            f'INSERT INTO "StoreStatus" (store_id, status, timestamp) VALUES ({store_status_row["store_id"]}, \'{store_status_row["status"]}\', \'{store_status_row["timestamp"]}\')')
        repair_store_status_connection.commit()
        repair_cursor.close()
    broken_store_count = 0
    broken_stores = []
    for row in tqdm(np.array(processed_store_status_dataframe), smoothing=0.9):
        insert_query = 'INSERT INTO "StoreStatus" (store_id, status, timestamp) VALUES (%s, %s, %s)'
        try:
            cursor.execute(insert_query, row)
            store_status_connection.commit()
        except Exception:
            store_status_connection.rollback()
            dict_row = {'store_id': row[0], 'status': row[1], 'timestamp': row[2]}
            broken_stores.append(dict_row)
            broken_store_count += 1

    for broken_store in tqdm(broken_stores, smoothing=0.9):
        repair_store_status_record(store_status_connection, broken_store)
    print(
        f'Inserted all store status ({len(processed_store_status_dataframe)}) and fixed {broken_store_count} broken(timezone-less) stores')
    cursor.execute('UPDATE "_DataProgress" SET store_status_populated = %s WHERE id = %s', (True, 1))
    store_status_connection.commit()
    cursor.close()


def process_timezones():
    print('Reading "timezones.csv"...')
    timezones = pd.read_csv('data_source/timezones.csv')
    timezones.fillna({'timezone_str': 'America/Chicago'}, inplace=True)
    timezones['timezone'] = timezones['timezone_str']  # .apply(lambda x: pytz.timezone(x))
    timezones.drop(columns=['timezone_str'], inplace=True)
    print(f'"Timezones" Row Count = {len(timezones)}')

    return timezones


def insert_timezones(timezones_connection, processed_timezones_dataframe):
    cursor = timezones_connection.cursor()
    skip_insertion = is_table_populated(timezones_connection, 'timezones_populated')
    if skip_insertion:
        return
    for row in tqdm(np.array(processed_timezones_dataframe), smoothing=0.9):
        insert_query = 'INSERT INTO "StoreTimezones" (store_id, timezone) VALUES (%s, %s)'
        cursor.execute(insert_query, row)
        timezones_connection.commit()
    print('Inserted all timezones!')
    cursor.execute('UPDATE "_DataProgress" SET timezones_populated = %s WHERE id = %s', (True, 1))
    timezones_connection.commit()
    cursor.close()


def create_tables(create_table_connection):
    _cursor = create_table_connection.cursor()

    def create_data_progress_db(cursor):
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS "_DataProgress" (
            "id" INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            "timezones_populated" BOOLEAN NOT NULL DEFAULT FALSE,
            "business_hours_populated" BOOLEAN NOT NULL DEFAULT FALSE,
            "store_status_populated" BOOLEAN NOT NULL DEFAULT FALSE
        );
        """)
        create_table_connection.commit()
        try:
            cursor.execute('INSERT INTO "_DataProgress" (id) VALUES (%s)', (1,))
            create_table_connection.commit()
        except Exception:
            create_table_connection.rollback()

    def create_store_timezones_db(cursor):
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS "StoreTimezones" (
            "store_id" BIGINT NOT NULL PRIMARY KEY,
            "timezone" TEXT NOT NULL
        );
        """)
        create_table_connection.commit()

    def create_store_business_hours_db(cursor):
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS "StoreBusinessHours" (
            "id" BIGSERIAL NOT NULL PRIMARY KEY,
            "store_id" BIGINT,
            "day_of_week" INT NOT NULL,
            "start_time_local" TIME NOT NULL,
            "end_time_local" TIME NOT NULL,
            
            FOREIGN KEY ("store_id") REFERENCES "StoreTimezones"("store_id") ON DELETE RESTRICT ON UPDATE CASCADE,
        
            CONSTRAINT "StoreBusinessHours_day_of_week_check" CHECK (day_of_week >= 0 AND day_of_week <= 6),
            CONSTRAINT "StoreBusinessHours_start_time_local_check" CHECK (start_time_local < end_time_local)
        );
        """)
        create_table_connection.commit()

    def create_store_status_db(cursor):
        # TODO: change status to enum(active,inactive)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS "StoreStatus" (
            "id" BIGSERIAL NOT NULL PRIMARY KEY,
            "store_id" BIGINT NOT NULL,
            "status" ENUM('active', 'inactive') NOT NULL,
            "timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        
            FOREIGN KEY ("store_id") REFERENCES "StoreTimezones"("store_id") ON DELETE RESTRICT ON UPDATE CASCADE
        );
        """)
        create_table_connection.commit()

    create_data_progress_db(_cursor)
    create_store_timezones_db(_cursor)
    create_store_business_hours_db(_cursor)
    create_store_status_db(_cursor)
    _cursor.close()
    print('Datastore prepared!')

def populate_tables(connection):
    processed_timezones = process_timezones()
    insert_timezones(connection, processed_timezones)
    processed_business_hours = process_business_hours()
    insert_business_hours(connection, processed_business_hours)
    processed_store_status = process_store_status()
    insert_store_status(connection, processed_store_status)


print('Connecting to the database...')
# Connect to the database
_connection = psycopg2.connect(
    host="localhost",
    database="loop_datastore_db",
    user="loop_datastore_admin",
    password="top_secret"
)
print('Connected!')
create_tables(_connection)
populate_tables(_connection)
print('Operation completed!!!')
print('Closing DB connection.')
_connection.close()

import pandas as pd
from typing import Tuple

from pytz.tzinfo import DstTzInfo
from tqdm import tqdm

from type_defs import StoreStatusList, StoreBusinessHoursListRaw, \
    StoreTimezoneListRaw, StoreStatusListRaw, Status, StoreBusinessHoursList, StoreBusinessHours
from datetime import time, datetime, timedelta
from pytz import timezone


def create_reporting_tables(create_table_connection) -> None:
    """
    Creates two tables in the given database connection for storing report status and report data.

    :param create_table_connection: A database connection object to create tables in.

    :return: None
    """
    cursor = create_table_connection.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "ReportStatus" (
        "id" UUID NOT NULL PRIMARY KEY,
        "status" TEXT NOT NULL DEFAULT 'Running',
        "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """)
    create_table_connection.commit()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "ReportData" (
        "id" SERIAL NOT NULL PRIMARY KEY,
        "report_id" UUID NOT NULL,
        "store_id" BIGINT NOT NULL,
        "uptime_last_hour" INT NOT NULL,
        "uptime_last_day" INT NOT NULL,
        "uptime_last_week" INT NOT NULL,
        "down_time_last_hour" INT NOT NULL,
        "down_time_last_day" INT NOT NULL,
        "down_time_last_week" INT NOT NULL,
        
        FOREIGN KEY ("report_id") REFERENCES "ReportStatus"("id")  ON DELETE RESTRICT ON UPDATE CASCADE
    );
    """)

    create_table_connection.commit()
    cursor.close()


def localize_time(time_string: str, local_timezone: DstTzInfo) -> time:
    """
    Converts a time string to a localized datetime object.

    :param time_string: A string representing the time to convert.
    :param local_timezone: A timezone object representing the timezone to localize the datetime to.

    :return: A datetime object representing the localized time.
    """
    time_instance = datetime.strptime(time_string, "%H:%M:%S").time()

    return datetime.combine(datetime.today(),
                            time_instance,
                            local_timezone.localize(datetime.today()).tzinfo).time()


def estimate_uptime_downtime(status_checks: StoreStatusList,
                             business_hours: StoreBusinessHours):
    """
    Estimate the uptime and downtime of a store based on its status checks and business hours.

    :param status_checks: A list of tuples representing the status checks for the store, 
                            where each tuple contains the timestamp and status of the check.
    :param business_hours: A tuple representing the business hours of the store, where the 
                            first element is the day of week, second element is start time 
                            and third element is end time.
    :return: A tuple containing the estimated uptime and downtime of the store in hours.
    """
    # Extract the day of week, start and end times of the business hours
    day_of_week, start_time_local, end_time_local = business_hours

    # Initialize variables to keep track of the uptime and downtime
    cumulative_uptime: timedelta = timedelta()
    cumulative_downtime: timedelta = timedelta()

    # Initialize variables to keep track of the previous status check
    prev_timestamp: datetime | None = None
    prev_status: Status | None = None

    current_time = start_time_local

    # Iterate over each hour within business hours
    while current_time < end_time_local:
        next_hour = datetime.combine(datetime.today(), current_time) + timedelta(hours=1)
        # Find the status checks that are within this hour on this day of the week
        hour_status_checks = [(timestamp, status) for timestamp, status in status_checks if
                              current_time <= timestamp.time() < next_hour.time() and timestamp.weekday() == day_of_week]

        # If there are no status checks within this hour, use linear interpolation to estimate the status
        if not hour_status_checks:
            if prev_timestamp is not None and prev_status == 'active':
                cumulative_uptime += timedelta(hours=1)
            elif prev_timestamp is not None and prev_status == 'inactive':
                cumulative_downtime += timedelta(hours=1)
            continue

        # Iterate over the status checks within this hour
        for i, (timestamp, status) in enumerate(hour_status_checks):
            # If this is the first status check within this hour, extrapolate backwards to the start of the hour
            if i == 0:
                duration = timestamp - current_time
                if status == 'active':
                    cumulative_uptime += duration
                else:
                    cumulative_downtime += duration

            # If this is not the first status check within this hour, interpolate between the previous and current status checks
            else:
                prev_timestamp, prev_status = hour_status_checks[i - 1]
                duration = timestamp - prev_timestamp
                if prev_status == 'active':
                    cumulative_uptime += duration
                else:
                    cumulative_downtime += duration

            # If this is the last status check within this hour, extrapolate forwards to the end of the hour
            if i == len(hour_status_checks) - 1:
                duration = next_hour - timestamp
                if status == 'active':
                    cumulative_uptime += duration
                else:
                    cumulative_downtime += duration

        # Update the previous status check variables
        prev_timestamp, prev_status = hour_status_checks[-1]

        # Increment the current time by one hour
        current_time += timedelta(hours=1)

    return cumulative_uptime, cumulative_downtime


def create_report(connection, report_id) -> None:
    """
    Creates a new report by inserting a new row into the "ReportStatus" table with a new UUID as 
        the ID.
    Then, generates the report data using a background worker and updates the status of the report.
    Finally, returns the ID of the newly created report.

    :param connection: A psycopg2 connection object to the database.

    Returns:
        A UUID string representing the ID of the newly created report.
    """
    create_reporting_tables(connection)
    cursor = connection.cursor()
    cursor.execute(f"""
        INSERT INTO "ReportStatus" (id) VALUES ('{report_id}'::uuid);
    """)
    connection.commit()
    cursor.close()


def generate_report_data(connection, report_id) -> None:
    """
    - Generates report data for the given report_id by querying data from the StoreTimezones, 
        StoreStatus and StoreBusinessHours tables.
    - Calculates the uptime and downtime for the last hour, day and week for each store and 
        inserts the data into the ReportData table.
    - Updates the status of the report to 'Completed' in the ReportStatus table.

    :param connection: A database connection object.
    :param report_id: An integer representing the id of the report.

    :return: None
    """
    cursor = connection.cursor()
    # Query the data from StoreTimezones tables
    cursor.execute("""
        SELECT "store_id", "timezone"
        FROM "StoreTimezones"
    """)

    stores: StoreTimezoneListRaw = cursor.fetchall()
    for store in tqdm(stores, smoothing=0.9):
        store_id: int = store[0]
        store_timezone: str = store[1]
        # Query the data from StoreStatus table as status_checks
        cursor.execute("""
            SELECT "timestamp", "status"
            FROM "StoreStatus"
            WHERE "store_id" = %s
        """, (store_id,))

        status_checks: StoreStatusListRaw = cursor.fetchall()

        # Query the data from StoreBusinessHours table
        cursor.execute("""
            SELECT "day_of_week", "start_time_local", "end_time_local"
            FROM "StoreBusinessHours"
            WHERE "store_id" = %s
        """, (store_id,))

        business_days = cursor.fetchall()

        # Calculate the uptime and downtime for the last hour, day and
        timezone_object = timezone(store_timezone)
        LAST_HOUR = 1
        LAST_DAY = 24
        LAST_WEEK = 168
        uptime_last_hour, downtime_last_hour = calculate_uptime_and_downtime(LAST_HOUR, business_days, status_checks,
                                                                             timezone_object)
        uptime_last_day, downtime_last_day = calculate_uptime_and_downtime(LAST_DAY, business_days, status_checks,
                                                                           timezone_object)
        uptime_last_week, downtime_last_week = calculate_uptime_and_downtime(LAST_WEEK, business_days, status_checks,
                                                                             timezone_object)
        uptime_last_hour_in_minutes = int(uptime_last_hour.total_seconds() // 60)
        downtime_last_hour_in_minutes = int(downtime_last_hour.total_seconds() // 60)

        uptime_last_day_in_hours = int(uptime_last_day.total_seconds() // 3600)
        downtime_last_day_in_hours = int(downtime_last_day.total_seconds() // 3600)

        uptime_last_week_in_hours = int(uptime_last_week.total_seconds() // 3600)
        downtime_last_week_in_hours = int(downtime_last_week.total_seconds() // 3600)

        cursor.execute("""
            INSERT INTO "ReportData" ("report_id", "store_id", "uptime_last_hour", "uptime_last_day", "uptime_last_week", "down_time_last_hour", "down_time_last_day", "down_time_last_week")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (report_id, store_id,
              uptime_last_hour_in_minutes,
              uptime_last_day_in_hours,
              uptime_last_week_in_hours,
              downtime_last_hour_in_minutes,
              downtime_last_day_in_hours,
              downtime_last_week_in_hours))
        connection.commit()

    cursor.execute("""
        UPDATE "ReportStatus" SET "status" = 'Completed' WHERE "id" = %s;
    """, (report_id,))
    connection.commit()
    cursor.close()


def localize_timestamp(timestamp: str, local_timezone: DstTzInfo) -> datetime:
    """
    Converts a UTC timestamp string to a datetime object with the specified local timezone.

    :param timestamp: A string representing a timestamp in the format '%Y-%m-%d %H:%M:%S.%f %Z'.
    :param local_timezone: A timezone object representing the local timezone.

    returns: A datetime object representing the converted timestamp with the specified local 
            timezone.
    """
    UTC_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f %Z'
    return datetime.strptime(timestamp, UTC_DATETIME_FORMAT).replace(tzinfo=local_timezone)


def calculate_uptime_and_downtime(duration_in_hours: int, business_days: StoreBusinessHoursListRaw,
                                  status_checks: StoreStatusListRaw, store_timezone: DstTzInfo) -> Tuple[
    timedelta, timedelta]:
    """
    Calculates the uptime and downtime for a store within a given duration.

    :param duration_in_hours: The duration in hours for which to calculate uptime and downtime.
    :param business_days: A list of tuples representing the business hours for each day of the week.
            Each tuple contains an integer representing the day of the week (0-6, where 0 is Monday and 6 is Sunday),
            a string representing the start time of the business hours in the format 'HH:MM', and a string representing
            the end time of the business hours in the format 'HH:MM'.
    :param status_checks: A list of tuples representing the status checks for the store.
            Each tuple contains a string representing the timestamp of the status check in the format 'YYYY-MM-DD HH:MM:SS.ssssss',
            and a string representing the status of the store ('up' or 'down').
    :param store_timezone: A timezone object representing the timezone of the store.

    :return: A tuple containing the uptime and downtime for the store within the given duration.
    """
    uptime_in_duration = downtime_in_duration = timedelta()
    now_local = datetime.utcnow().astimezone(store_timezone)
    last_duration_start_time = (now_local - timedelta(hours=duration_in_hours)).replace(minute=0, second=0,
                                                                                        microsecond=0)
    last_duration_end_time = last_duration_start_time + timedelta(hours=duration_in_hours)
    last_duration_business_days: StoreBusinessHoursList \
        = [(day_of_week, localize_time(start_time_local, store_timezone),
            localize_time(end_time_local, store_timezone)) for
           day_of_week, start_time_local,
           end_time_local in business_days if
           day_of_week == last_duration_start_time.weekday()]

    last_duration_status_checks: StoreStatusList \
        = [(localize_timestamp(timestamp, store_timezone), status) for
           timestamp, status in status_checks if
           last_duration_start_time <= localize_timestamp(timestamp, store_timezone) < last_duration_end_time]
    for business_hours in last_duration_business_days:
        response = estimate_uptime_downtime(last_duration_status_checks, business_hours)
        uptime_in_duration += timedelta(hours=response[0].total_seconds() // 3600)
        downtime_in_duration += timedelta(hours=response[1].total_seconds() // 3600)
    return uptime_in_duration, downtime_in_duration


def is_report_completed(connection, report_id: str) -> bool | None:
    """
    Checks if the status of the report with the given ID is "Completed".

    :param report_id: A UUID string representing the ID of the report.

    :return: A bool representing status of the report or None if the report does not exist.
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT "status" FROM "ReportStatus" WHERE "id" = %s;
        """, (report_id,))
        if cursor.rowcount == 0:
            return None
        status = cursor.fetchone()[0]

    return status == 'Completed'


def get_report_data(connection, report_id: str):
    """
    Gets the report data for the given report ID.

    :param report_id: A UUID string representing the ID of the report.

    :return: A list of tuples representing the report data.
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT "store_id", "uptime_last_hour", "uptime_last_day", "uptime_last_week", "down_time_last_hour", "down_time_last_day", "down_time_last_week"
            FROM "ReportData"
            WHERE "report_id" = %s
        """, (report_id,))
        report_data = cursor.fetchall()

    return report_data


def convert_to_csv(report_data):
    """
    Converts the given report data to a CSV file.

    :param report_data: A list of tuples representing the report data.

    :return: A string representing the CSV file.
    """
    data_frame = pd.DataFrame(report_data,
                              columns=["store_id",
                                       "uptime_last_hour",
                                       "uptime_last_day",
                                       "uptime_last_week",
                                       "down_time_last_hour",
                                       "down_time_last_day",
                                       "down_time_last_week"])
    return data_frame.to_csv(index=False)

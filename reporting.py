import uuid

from pytz.tzinfo import DstTzInfo

from type_defs import StoreStatusList, StoreBusinessHoursListRaw, \
    StoreTimezoneListRaw, StoreBusinessHoursRaw, StoreStatusListRaw, Status
from datetime import time, datetime, timedelta
from pytz import timezone


def create_reporting_tables(create_table_connection):
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


def estimate_uptime_downtime(status_checks: StoreStatusList,
                             business_hours: StoreBusinessHoursRaw,
                             store_timezone: DstTzInfo):
    """
    Estimate the uptime and downtime of a store based on its status checks and business hours.

    :param status_checks: A list of tuples representing the status checks for the store, where each tuple contains the timestamp and status of the check.
    :param business_hours: A tuple representing the business hours of the store, where the first element is the day of week, second element is start time and third element is end time.
    :return: A tuple containing the estimated uptime and downtime of the store. (time deltas in hours)
    """
    # Extract the day of week, start and end times of the business hours
    day_of_week: int = business_hours[0]
    start_time_local: time = datetime.strptime(business_hours[1], "%H:%M:%S").time()
    end_time_local: time = datetime.strptime(business_hours[2], "%H:%M:%S").time()

    start_datetime = datetime.combine(datetime.today(), start_time_local,
                                      store_timezone.localize(datetime.today()).tzinfo)
    end_datetime = datetime.combine(datetime.today(), end_time_local, store_timezone.localize(datetime.today()).tzinfo)

    # Initialize variables to keep track of the uptime and downtime
    cumulative_uptime: timedelta = timedelta()
    cumulative_downtime: timedelta = timedelta()

    # Initialize variables to keep track of the previous status check
    prev_timestamp: datetime | None = None
    prev_status: Status | None = None

    current_time = start_datetime

    # Iterate over each hour within business hours
    while current_time < end_datetime:
        # Find the status checks that are within this hour on this day of the week
        hour_status_checks = [(timestamp, status) for timestamp, status in status_checks if
                              current_time <= timestamp < (current_time + timedelta(
                                  hours=1)) and timestamp.weekday() == day_of_week]

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
                duration = (current_time + timedelta(hours=1)) - timestamp
                if status == 'active':
                    cumulative_uptime += duration
                else:
                    cumulative_downtime += duration

        # Update the previous status check variables
        prev_timestamp, prev_status = hour_status_checks[-1]

        # Increment the current time by one hour
        current_time += timedelta(hours=1)

    return cumulative_uptime, cumulative_downtime


def create_report(connection):
    create_reporting_tables(connection)
    cursor = connection.cursor()
    cursor.execute(f"""
    INSERT INTO "ReportStatus" (id) VALUES ('{uuid.uuid4()}'::uuid) RETURNING id;
    """)
    connection.commit()
    report_id = cursor.fetchone()[0]
    # TODO - Use a background worker to generate the report data
    #   and update the status of the report, then use the /get_report endpoint
    #   to retrieve the report data
    generate_report_data(connection, report_id)
    cursor.close()
    return report_id


def generate_report_data(connection, report_id):
    cursor = connection.cursor()
    # Query the data from StoreTimezones tables
    cursor.execute("""
        SELECT "store_id", "timezone"
        FROM "StoreTimezones"
        LIMIT 5
    """)

    stores: StoreTimezoneListRaw = cursor.fetchall()
    for store in stores:
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


def calculate_uptime_and_downtime(duration_in_hours: int, business_days: StoreBusinessHoursListRaw,
                                  status_checks: StoreStatusList, store_timezone: DstTzInfo):
    uptime_in_duration = downtime_in_duration = timedelta()
    now_utc = datetime.utcnow()
    now_local = now_utc.astimezone(store_timezone)
    last_duration_start_time = (now_local - timedelta(hours=duration_in_hours)).replace(minute=0, second=0,
                                                                                        microsecond=0)
    last_duration_end_time = last_duration_start_time + timedelta(hours=duration_in_hours)
    last_duration_business_days: StoreBusinessHoursListRaw \
        = [(day_of_week, start_time_local, end_time_local) for
           day_of_week, start_time_local,
           end_time_local in business_days if
           day_of_week == last_duration_start_time.weekday()]
    UTC_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f %Z'
    str_to_datetime = lambda x: datetime.strptime(x, UTC_DATETIME_FORMAT).replace(tzinfo=store_timezone)
    last_duration_status_checks: StoreStatusList \
        = [(str_to_datetime(timestamp), status) for
           timestamp, status in status_checks if
           last_duration_start_time <= str_to_datetime(timestamp) < last_duration_end_time]
    for business_hours in last_duration_business_days:
        response = estimate_uptime_downtime(last_duration_status_checks, business_hours, store_timezone)
        uptime_in_duration += timedelta(hours=response[0].total_seconds() // 3600)
        downtime_in_duration += timedelta(hours=response[1].total_seconds() // 3600)
    return uptime_in_duration, downtime_in_duration

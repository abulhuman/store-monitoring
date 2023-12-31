# Solution Steps
*Considering the details provided in the task page, here are some cases that we need to keep in mind.*
1. The input is 3 files which were provided in the task page. Namely, `timezones.csv`, `business_hours.csv` 
and `store_status.csv`.
   1. The `timezones.csv` file contains the timezone of each store. Further, it is also the only file which 
contains unique values for the `store_id` field. So we can use the values in this file as a representation 
of stores.
   1. The `business_hours.csv` file contains the business hours of each store. The `store_id` field is not 
unique in this file. This means that there are multiple entries for each store. Further, I discovered that 
it has records with `store_id` values which are not present in the `timezones.csv` file. This means that 
there are some stores which are present in `business_hours.csv` but not in `timezones.csv`. Indicating to us
that we have a store with business hours provided with an unset timezone. As provided in the output 
requirements, we need to set the timezone to `'America/Chicago'`. This can be found inside the 
`repair_business_hours_record` method of `sequelize.py`. 
   1. The `store_status.csv` file contains the status of each store. The `store_id` field is not unique in 
this file. This means that there are multiple entries for each store. Further, I discovered that it ***also*** 
has records with `store_id` values which are not present in the `timezones.csv` file. This means that there
are some stores which are present in `store_status.csv` but not in `timezones.csv`. Indicating to us that we
have a store with status provided with an unset timezone. As provided in the output requirements, we need to
set the timezone to `'America/Chicago'` and the business hours of these stores to `24/7`. This can be found 
inside the `repair_store_status_record` method of `sequelize.py`.
   1. After the insertion of the records, we can start to focus on the output requirements.
1. The output is an API with two routes, `/trigger_report` and `/get_report`. Both routes are `GET` routes.
   1. The `/trigger_report` route is used to trigger the report generation. It takes in no parameters. It
returns a `JSON` response with the `report_id` of the report that was generated. This `report_id` is used to
poll and retrieve the report from the `/get_report` route.
   > The requirements clearly state that the `/trgger_report` route should return a string response. However,
    I decided to return a `JSON` response with the `report_id` as the `JSON` key. This is because I wanted to
    make the API more extensible. If we were to add more information to the report, we can simply add more keys
     to the `JSON` response. This would not be possible if we were to return a string response.

   > The requirements also state that the `/trigger_report` route should return a `report_id` which is a
    random string. So I decided to use the `uuid` library to generate a random string. This is because the
    `uuid` library is a standard library in Python and is used to generate random strings. This is also
    because the `uuid` library is a cryptographically secure random string generator. This means that the
    `report_id` generated is unique and cannot be guessed.
   1. The `/get_report` route is used to retrieve the report generated by the `/trigger_report` route. It
takes in a `report_id` parameter. It returns a `csv` file named `{report_id}.csv` with the report data if 
the report generation is "Complete" but if not, it returns the text "Running".
      1. The `{report_id}.csv` file is generated using the `generate_report` method of `reporting.py`. This
method uses the `report_id` to query the database for the report data. The report data is then converted into
a `csv` file using the `convert_to_csv` method of `reporting.py`.
      1. The schema for the report data is as follows:
         1. The `store_id` field is the `store_id` of the store.
         1. The `uptime_last_hour` field is  (in minutes) the uptime of the store in the last hour.
         1. The `uptime_last_day` field  (in hours) is the uptime of the store in the last day.
         1. The `uptime_last_week` field is  (in hours) the uptime of the store in the last week.
            > The requirements state that the name of this column as `update_last_week` but the corresponding "down" 
             version was named `downtime_last_week`. So I decided to change the name of this column to `uptime_last_week`
             to make it consistent with the naming convention.
         1. The `downtime_last_hour` field is  (in minutes) the downtime of the store in the last hour.
         1. The `downtime_last_day` field is (in hours) the downtime of the store in the last day.
         1. The `downtime_last_week` field is  (in hours) the downtime of the store in the last week.
      1. The entire database schema is as follows:
         1. Table `"Timezones"`:
            1. `store_id (BIGINT)` - The `store_id` of the store.
            1. `timezone (TIMESTAMP WITH OUT TIMEZONE)` - The timezone of the store.
         1. Table `"BusinessHours"`:
            1. `id (INT)` - The unique id for the record. I made it an `INT` for faster indexing.
            1. `store_id (BIGINT)` - The `store_id` of the store.
            1. `day_of_week (INT(0-6))` - The day of the week.
            1. `start_time_local (TIME)` - The start time for the store (beginning of the business hours for that day).
            1. `end_time_local (TIME)` - The end time for the store (end of the business hours for that day).
         1. Table `"StoreStatus"`:
            1. `id (INT)` - The unique id for the record. I made it an `INT` for faster indexing.
            1. `store_id (BIGINT)` - The `store_id` of the store.
            1. `status (enum {active, inactive})` - The activity status (open or closed)-ness of the store.
            1. `timestamp (TIMESTAMP WITHOUT TIME ZONE)` - Timestamp in UTC when the status check polling was performed.
         1. Table `"_DataProgress"` - A utility table to keep track of the insertion status when populating that db from
the csv files:
            1. `id (INT)` - ...
            1. `timezones_populated (BOOLEAN)` - Tracks insertion status for records inside `Timezones` table.
            1. `business_hours_populated (BOOLEAN)` - Tracks insertion status for records inside `BusinessHours` table.
            1. `store_status_populated (BOOLEAN)` - Tracks insertion status for records inside `StoreStatus` table.
  
## Data Preprocessing
1. The data preprocessing is done using the `sequelize.py` file. It contains three stages of operation:
   1. The first one being the creation of all the database tables. This is done using the `create_tables` method.
   1. The second one being the population of the database tables. This is done using the `populate_tables` method.
      1. The `populate_tables` method uses the `insert_timezones`, `insert_business_hours` and `insert_store_status`
         1. The `insert_timezones` method inserts the records from the `timezones.csv` file into the `Timezones` table.
         1. The `insert_business_hours` method inserts the records from the `business_hours.csv` file into the 
`BusinessHours` table.
         1. The `insert_store_status` method inserts the records from the `store_status.csv` file into the `StoreStatus`
table.
         1. The `repair_business_hours_record` method repairs the records in the `BusinessHours` table which have an 
unset timezone.
         1. The `repair_store_status_record` method repairs the records in the `StoreStatus` table which have an unset 
timezone.
         1. The `repair_store_status_record` method also repairs the records in the `StoreStatus` table which have an 
unset business hours.
   1. At this point, the database is populated with the data from the csv files. We can now start to focus on the output
requirements.

## Report Generation
**Before we dive deep into writing report generation logic, let us first discuss some considerations.**
To implement report generation logic the first thing we need to do is to understand the requirements. 
- The requirements state that we need to generate a report which contains the *uptime and downtime of each store in the 
last hour, day and week.*
This means that we need to ***find a way to calculate*** the uptime and downtime of each store in the last hour, day and
week.
- The requirements also state that uptime and downtime should only include observations within business hours. This means
that we need to ***find a way to filter out observations which are outside business hours.***
- The requirements also state that need to extrapolate uptime and downtime based on the data inside `"StoreStatus"` 
table, to the entire time interval. This means that we need to ***find a way to extrapolate*** the uptime and downtime 
of each store in the last hour, day and week. So the solution roughly involves the following steps:
1. Calculate the uptime and downtime of each store in the last hour, day and week. Based on the data inside 
`"StoreStatus"` table.
1. Filter out observations which are outside business hours.
1. Extrapolate the uptime and downtime of each store in the last hour, day and week to the entire time interval based on 
some logic.
1. Interpolate the uptime and downtime of each store in the last hour, day and week to the entire time interval based on
some sane logic or algorithm.
1. Generate and store a report which contains all the required information described in the requirements.

> The hardest part of this problem is to find a way to extrapolate the uptime and downtime of each store in the last
> hour, day and week to the entire time interval. This is because we do not have any information about the uptime and 
> downtime of each store in the last hour, day and week. We only have information about the uptime and downtime of each 
> store at a particular timestamp. So we need to find a way to extrapolate the uptime and downtime of each store in the 
> last hour, day and week to the entire time interval based on some logic. The first thing that came to my mind is that 
> I could use the `business_hours` provided to see at which percentage point does the status check lie and then 
> extrapolate the uptime and downtime based on that. For example, if the status check lies in the first 50% of the
> business hours, then I could extrapolate the uptime and downtime to the entire time interval by multiplying the uptime
> and downtime by 2. However, this approach is not very accurate. This is because the status check could lie in the first
> 50% of the business hours but the store could have been down for the entire time interval. So I decided to look for a
> better approach. After a lot of research, I found a few ways to do this extrapolation. Some being better than others 
> in different aspects; accuracy, speed (computationally heavy), technical complexity, development speed, historical 
> data-hungry, etc. Just to mention a few  interpolation methods that we can use to estimate the uptime and downtime 
> of a store based on its status checks:
> - ***Linear interpolation***: This method estimates the value of a function at an intermediate point by connecting two 
known points with a straight line. It is simple to implement and can provide reasonable estimates when the data 
is relatively smooth and linear.
> - ***Polynomial interpolation***: This method estimates the value of a function at an intermediate point by fitting a 
polynomial curve to a set of known points. It can provide more accurate estimates than linear interpolation when 
the data is non-linear, but it can also be more computationally intensive.
> - ***Spline interpolation***: This method estimates the value of a function at an intermediate point by fitting a piecewise 
polynomial curve to a set of known points. It can provide smooth and accurate estimates, and is often used when the data
has complex non-linear relationships.
> - ***Nearest-neighbor interpolation***: This method estimates the value of a function at an intermediate point by assigning 
it the value of the nearest known point. It is simple to implement, but can produce discontinuous and blocky estimates.  
***Note:** In addition, there are other techniques such as historical data and machine learning algorithms, but implementing 
them requires a lot more resources and data than what we have here.*
> > We are going to implement the `Linear Interpolation` method. This is because it is simple to implement and can provide
reasonable estimates when the data is relatively smooth and linear. This is also because it is computationally less
expensive than the other methods. We should be aware that this method is not very accurate. This is because it assumes
that the data is linear and smooth. However, we can use this method to get a rough estimate of the uptime and downtime
of each store in the last hour, day and week. We can then use this rough estimate to generate the report. If we want
to improve the accuracy of the report, we can implement the other methods mentioned above and use them based on
the characteristics of the data such as smoothness, linearity, mean, median, variance, standard deviation, etc.
our algorithm is implemented inside the `estimate_uptime_and_downtime` method of `reporting.py`. It takes in the 
`business_hours` and `status_checks` of a store and iterates over each hour within business hours and finds the 
status checks (store status) that are within that hour on that day of the week. If there are no statuses within that hour
on that day, we use linear interpolation to estimate whether the store was `active` or `inactive` based on its previous 
status. If there are one or more store statuses within that hour, we use linear interpolation to estimate the uptime and
downtime between those status checks. This approach allows us to fill in missing data during business hours by using 
linear interpolation to estimate whether a store was `active` or `inactive` based on its previous and subsequent store 
statuses.

**Now that we have discussed the considerations, let us dive deep into writing report generation logic.**
- The report generation logic is done using the `reporting.py` file. It contains a few methods:
   - The `create_report` method is used to create the report. It takes no parameters. It returns a `report_id` which is 
a random string (UUID).
      - Creates a new report by inserting a new row into the "ReportStatus" table with a new UUID as 
        the ID.
      - Then, generates the report data using a background worker thread and updates the status of the report.
      - Finally, returns the ID of the newly created report. 
   - The `generate_report_data` method is used to generate the report data. It takes in a `report_id` parameter. It
returns the report data.
      - Queries the database for the store statuses.
      - Queries the database for the business hours.
      - Iterates over each store and calculates the uptime and downtime of each store in the last hour, day and week.
      - Saves a new row into the "ReportData" table with the report data for each store.
      - Updates the status of the report.
   - The `get_report_data` method is used to query the database for the report data. It takes in a `report_id` 
parameter. It returns the report data.
      - Queries the database for the report data.
      - Returns the report data.
   - The `convert_to_csv` method is used to convert the report data into a `csv` (comma separated values, ready to be 
written to an in-memory file and sent to the client immediately as download to not use up storage). It takes in a 
`report_id` parameter. It returns a `csv` file named `{report_id}.csv` with the report data.
   - The `generate_report` method uses the `get_report_data` method to query the database for the report data. It then 
  uses the `convert_to_csv` method to convert the report data into a `csv` file.
  

## API
- The API is done using the `api.py` file. It contains two routes:
   - The `/trigger_report` route is used to trigger the report generation. It takes in no parameters. It returns a `JSON`
response with the `report_id` of the report that was generated.
     - The `report_id` is a random string (UUID).
     - The `report_id` is used to poll and retrieve the report from the `/get_report` route.
     - This uses the `BackgroundTask` class to run the `create_report` and the `generate_report_data` method of 
`reporting.py` in the background.
   - The `/get_report` route is used to retrieve the report generated by the `/trigger_report` route. It takes in a
`report_id` parameter. It returns a `csv` file named `{report_id}.csv` with the report data if the report generation is
"Complete" but if not, it returns the text "Running".
     - The `report_id` is used to query the database for the report data.
     - The `report_id` is used to query the database for the status of the report.
     - If the status of the report is "Complete", it uses the `convert_to_csv` method of `reporting.py` to convert the
report data into a `csv` string.
     - If the status of the report is not "Complete", it returns the text "Running".



      

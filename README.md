# Loop AI - Take Home Interview

## Introduction
The problem statement and introduction can be found at [this notion page.](https://loopxyz.notion.site/Take-home-interview-Store-Monitoring-12664a3c7fdf472883a41457f0c9347d)  

## Solution
The solution is implemented in Python 3.11. The solution is divided into 3 parts:
1. [Data Preprocessing](#data-preprocessing)  
< to be completed >  
### Solution Instructions
1. Clone the repository
1. Install the dependencies using `make i`
1. Make a local version of the `.env` file using `cp .env.example .env`
1. Edit the `.env` file to add your database credentials
1. Start your docker database using `make up-db`
1. Download the data from the path mentioned in the [problem statement](https://loopxyz.notion.site/Take-home-interview-Store-Monitoring-12664a3c7fdf472883a41457f0c9347d)
1. Save the store timezone file as `data_source/timezones.csv`
1. Save the store business hours file as `data_source/business_hours.csv`
1. Save the store status file as `data_source/store_status.csv`
1. Run the data processing portion of the solution using `make sequelize`
1. Run the fastAPI server using `make run`
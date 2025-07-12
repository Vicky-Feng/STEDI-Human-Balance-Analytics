# STEDI-Human-Balance-Analytics
Repo for WGU/Udacity D609 "Data Analytics at Scale" project submission. Project rubric addressed as follows:

## Required files

Required python files at top level of repository:
- customer_landing_to_trusted.py
- accelerometer_landing_to_trusted.py
- step_trainer_trusted.py
- customer_trusted_to_curated.py
- machine_learning_curated.py

SQL queries in SQL_DDL_scripts directory:
-  customer_landing.sql
-  accelerometer_landing.sql
-  step_trainer_landing.sql

## Screenshots
### Landing Zone

![Count of customer_landing: 956 rows](./screenshots/count_of_customer_landing.png)
![The customer_landing data contains multiple rows with a blank shareWithResearchAsOfDate.](./screenshots/multiple_blank_shareWithResearchAsOfDate_in_customer_landing.png)
![Count of accelerometer_landing: 81273 rows](./screenshots/count_of_accelerometer_landing.png)
![Count of step_trainer_landing: 28680 rows](./screenshots/count_of_accelerometer_landing.png)

### Trusted Zone

![Count of customer_trusted: 482 rows](./screenshots/count_of_customer_trusted.png)
![Count of customer_trusted: 0 rows where shareWithResearchAsOfDate is blank](./screenshots/no_null_ shareWithResearchAsOfDate_customer_trusted.png)
![Count of accelerometer_trusted: 40981 rows](./screenshots/count_of_accelerometer_trusted.png)
![Count of step_trainer_trusted: 14460 rows](./screenshots/count_of_step_trainer_trusted.png)

### Curated Zone

Screenshots:
![Count of customer_curated: 482 rows](./screenshots/count_of_customer_curated.png)
![Count of machine_learning_curated: 43681 rows](./screenshots/count_of_machine_learning_curated.png)

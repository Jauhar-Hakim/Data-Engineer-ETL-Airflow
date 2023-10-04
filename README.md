# Data-Engineer-Test
Demonstrating data engineering skill in sakila database at MySQL website. Make new star scheme database based on sakila database and Airflow DAGs that run daily.

## Original Sakila Database ERR Diagram
![origin-sakila-err-diagram](https://github.com/Jauhar-Hakim/Data-Engineer-Test/assets/71159391/90e5ed19-1b9b-4cfa-bac2-5ebfeccba8c4)

## New Star Schema Sakila Databse ERR Diagram
![star-sakila-err-diagram](https://github.com/Jauhar-Hakim/Data-Engineer-Test/assets/71159391/64b10cec-ab0b-4831-80aa-1df8b8fa5c33)

# File Property
- Script-Transforming-Sakila-To-Star-Scheme-Databases.sql

  > SQL queries script to transform original sakila database to new star scheme sakila database for the first time and one time only
  
- Script-Transforming-Sakila-To-Star-Scheme-Databases.ipynb

  > SQL queries script to transform original sakila database to new star scheme sakila database for the first time and one time only
  In IPYNB file to make more human readable
  
- airflow-script-transform-daily.py

  > Python script that using Airflow DAGs to make script that can run daily with flow to transform new data from old database sakila to new database star scheme sakila.

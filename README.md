# DBtransfer
Insight Data Engineering project, Seattle 2019C session

## Project Idea/Business Value
How can we transfer the contents of a large database from one format to another with minimal downtime, while allowing additions and updates to the original database? I'll present a method to do this and to validate that the final information is the same between the databases.

Specific business case to transfer from non-relational to relational database?

## Tech Stack
Two database formats: MongoDB, MySQL  
Transfer tool: Presto, Hive, Pig?  
Monitor tool: Airflow

## Data Source
Example: Amazon customer reviews  
https://s3.amazonaws.com/amazon-reviews-pds/readme.html

Allow realtime updates and additions to the original database while transfer is occuring (simulate this behavior by holding back some records from the original database)

## Engineering Challenge
How do we make sure that an update to a record that has already been transferred is applied to the new database as well?  
How do we verify the databases contain the same information?

## MVP
- Load data into initial database  
- Transfer into a second database without allowing changes
  - This will be a benchmark for the amount of time and resources to allow a simple transfer if the database is closed during transfer
- Set up process to change initial database during transfer  
- Create processes to: (1) apply updates to both databases; (2) verify databases have the same data

## Stretch Goals
Provide functionality for multiple database formats  
Improve speed without compromising functionality (or at least quantify the tradeoff)

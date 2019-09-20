# DBtransfer
Insight Data Engineering project, Seattle 2019C session  
Carl Ulberg  
[Demo Link](https://docs.google.com/presentation/d/1w9XPYJcbFgoh0h54k6VQ_GO_wmYYBmX3PBumH-rveso/edit?usp=sharing)

## Project Idea/Business Value
Companies often need to upgrade or change their data storage capabilities. Taking a database offline to do this can cost money by wasting time of internal users or potentially losing external users. At Insight, I have developed a framework for quickly migrating a live database between different formats while ensuring consistency between them. 

Needed: Specific business case to transfer from non-relational to relational database?  
Example: Coursera did this in ~2012 (from MongoDB to MySQL) because they wanted more rigid schema

## Tech Stack
Two database formats: MongoDB, MySQL  
Transfer tool: Spark (batch), Kafka (realtime)  
Monitor tool: Airflow (to check consistency)

![Tech stack image](https://github.com/ulbergc/DBtransfer/blob/master/img/TechStack1.png)

## Data Source
Example: Amazon customer reviews  
https://s3.amazonaws.com/amazon-reviews-pds/readme.html

Allow realtime updates and additions to the original database while transfer is occuring (simulate this behavior by holding back some records from the original database)

## Engineering Challenge
How do we make sure that an update to a record that has already been transferred is applied to the new database as well?  
How do we verify the databases contain the same information?

## MVP
- Load data into initial database (done)  
- Transfer into a second database without allowing changes
  - This will be a benchmark for the amount of time and resources to allow a simple transfer if the database is closed during transfer
  - Set up second database (done)
  - Spark cluster set up and reading information from first database (done)
  - Spark cluster writes information to second database
- Set up process to change initial database during transfer  
- Create processes to: (1) apply updates to both databases; (2) verify databases have the same data

## Stretch Goals
Provide functionality for multiple database formats  
Improve speed without compromising functionality (or at least quantify the tradeoff)

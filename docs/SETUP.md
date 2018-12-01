# Setting up AWS EC2 cloud computer and MySQL data-base

## Introduction 
This code repository runs from an Amazon EC2 Linux instance and stores data in an Amazon RDB MySQL instance. The backend setup files are located in the __backend__ folder in this repository. The __backend__ folder consists of two sub-folders (__db__ and __ec2__), which contain files to set up the data-base and the EC2 instance respectively. 

## Data-base 
The GMA project makes use of an Amazon MySQL t2.small instance. The AWS setup for this instance type is standard. This is where all scraped data for NREGA and Shaalakosh is stored. The first file in the sub-folder __db__ is __db_schema.py__. This file is used during setup to create the tables that we need for the data-base using __sqlalchemy__. It also has a helper function called __get_table_keys__ which gets all the column names from the table and stores them in a .json file called __table_keys.json__. 

## EC2 instance
### SCP secrets file



## Errors and solutions 
### GCC

### 




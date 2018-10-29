# ETL for Gates Mobile Access

## Overview

This repository contains scripts for the Gates Mobile Access Mor Awaaz push call ETL. Every week for 52 weeks, 7200 SKY programme beneficiaries in Raipur district, Chattisgarh will recieve a push call from our IVRS system. An automated voice will give these beneficiaries information about their household's pending NREGA payments. This information will include the total amount due to the household, which payment stage the pending transaction has reached, and the contact information of the Gram Rozgar Sevak in their panchayat if the payment has been rejected. 

The NREGA payments process captures these variables on documents called Fund Transfer Orders (FTOs). FTOs are generated at the block office in each block and uploaded to the NREGA FTO tracking system. The scrape visits the FTO tracking system every week to scrape all the FTOs in Raipur district. The scraped data gets used to create calling scripts, which are then sent to our calling platform so that the push calls can be made.  

## SKY programme

The SKY programme is a Chattisgarh Government initiative to expand access women's access to mobile phones in the state. Under this programme, 50,000 women received free mobile phones from MicroMax and phone connectivity from RelianceJio. The beneficiaries recieve phones at distribution camps. For this project, the survey team sampled 7200 women from among these beneficiaries in Raipur district. As part of the intevention, our survey team is carrying out supplementary enrollment camps which train women to use the phones they have recieved and explain what to expect in the push calls. 

## NREGA FTO Tracking System

The NREGA payments process tracks each transaction that has to be made under NREGA. This information is digitized and stored in the FTO status tracking system. 

## Main scripts 

#### nrega_etl.sh 

Every morning at 730 am, the ETL shell script nrega_etl.sh is triggered by a cron job on an Amazon EC2 instance. This shell script runs a Python program which runs the following scripts in the given order. 

#### fto_stats.py 

This script is run from nrega_etl.sh as soon as the cron triggers the script. It takes the following steps: 

* Visits the block level summary page for Raipur district 
* Scrapes the total number of FTOs at each stage
* Follows the links in each of the cell in the block-level summary page
* Scrapes the list of FTO numbers for each block for each stage from the hyperlinked pages 
* Writes the scraped list to DB 

#### fto_filter.py

This script is run from nrega_etl.sh as soon as fto_stats.py finishes. It filters the scraped FTO numbers to get only those FTOs that are needed for the call in that week. It takes the following steps: 

* FTOs are listed for each stage they have been through so we keep only FTO under the furthest stage
* Removes any FTOs older than 7 days 
* Queries the DB for the FTOs that have already been scraped 
* Removes these from the list of FTOs to be scraped 
* Queries the DB for FTOs for whom information has already been pushed
* Removes any FTOs for whom information has already been pushed 
* Writes to SQL DB table with list of FTOs to be scraped 

#### fto_content.py 

This script is run from nrega_etl.sh as soon as fto_filter.py finishes. It visits the pages of the FTO numbers in the queue to actually scrape the content of each FTO and write them to the data-base. 

#### items.py 

#### pipelines.py 

#### helpers.py 

#### db_schema.py 

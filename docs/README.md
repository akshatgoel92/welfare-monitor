# ETL for Gates Mobile Access

## Overview

This repository contains scripts for the Gates Mobile Access Mor Awaaz push call ETL. Every week for 52 weeks, 7200 SKY programme beneficiaries in Raipur district, Chattisgarh will recieve a push call from our IVRS system. An automated voice will give these beneficiaries information about their household's pending NREGA payments. This information will include the total amount due to the household, which payment stage the pending transaction has reached, and the contact information of the Gram Rozgar Sevak in their panchayat if the payment has been rejected. 

The NREGA payments process captures these variables on documents called Fund Transfer Orders (FTOs). FTOs are generated at the block office in each block and uploaded to the NREGA FTO tracking system. The scrape visits the FTO tracking system every week to scrape all the FTOs in Raipur district. The scraped data gets used to create calling scripts, which are then sent to our calling platform so that the push calls can be made.  

## SKY programme

The SKY programme is a Chattisgarh Government initiative to expand access women's access to mobile phones in the state. Under this programme, 50,000 women received free mobile phones from MicroMax and phone connectivity from RelianceJio. The beneficiaries recieve phones at distribution camps. For this project, the survey team sampled 7200 women from among these beneficiaries in Raipur district. As part of the intevention, our survey team is carrying out supplementary enrollment camps which train women to use the phones they have recieved and explain what to expect in the push calls. 

## NREGA FTO Tracking System

The NREGA payments process tracks each transaction that has to be made under NREGA. This information is digitized and stored in the FTO status tracking system. 

## nrega_etl.sh 

Every morning at 730 am, the ETL shell script nrega_etl.sh is triggered by a cron job on an Amazon EC2 instance. The program flows as follows: 

* Visits the block level summary page for Raipur district and scrape the total no. of FTOs at each stage. This is used for data quality checks later. 
* Clicks on each of the hyperlinks on the block level summary page, which lead it to a list of FTOs for that block and payment stage. This list of FTOs is scraped and placed on the DB. 
* The list of FTOs is then filtered for FTOs that entered the payment processing system in the previous week and which have not been scraped    yet.
* These FTOs are placed in the FTO queue table to be scraped. Once this is done, the the FTOs themselves are scraped.

## fto_filter.py


## fto_stats.py 

## fto_content.py 

## items.py 

## pipelines.py 


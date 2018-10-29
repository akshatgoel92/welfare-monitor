# ETL for Gates Mobile Access

This repository contains scripts for the Gates Mobile Access Mor Awaaz push call ETL. Every week for 52 weeks, 7200 SKY programme beneficiaries in Raipur district, Chattisgarh will recieve a push call from our IVRS system. An automated voice will give these beneficiaries information about their household's pending NREGA payments. This information will include the total amount due to the household, which payment stage the pending transaction has reached, and the contact information of the Gram Rozgar Sevak in their panchayat if the payment has been rejected. 

These variables are found on Fund Transfer Orders (FTOs). FTOs are generated at the block office in each block and uploaded to the NREGA FTO tracking system. The scrape visits the FTO tracking system every week to scrape all the FTOs in Raipur district. The scraped data gets used to create calling scripts, which are then sent to our calling platform so that the push calls can be made.  

# SKY programme

The SKY programme is a Chattisgarh Government initiative to expand access women's access to mobile phones in the state. Under this programme, 50,000 women recieved free mobile phones from MicroMax and phone connectivity from RelianceJio. For this project, the survey team sampled 7200 women from among these beneficiaries in Raipur district.

# NREGA FTO Tracking System 


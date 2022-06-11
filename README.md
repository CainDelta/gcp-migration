# gcp-migration
Migrate Google Cloud sheets to BigQuery and create cloud function that updates the data using the UKFoodRating API

##### PROJECT BREAKDOWN 

Goal is to find an efficient way to unify 400+ google sheets containing data from all the restaurants in the UK into one location and upload that into BigQuery then proceed to use the UKfoodRating API to create a script that will first, update the dataset with new data, then make updates where necessary.

##### TECH USED 

- Python 
- GCP BigQuery
- GCP Cloud Function
- GCP Pub/Sub 
- GCP Orchestrator 

Orcherstrator triggers a Pub/Sub job which in turn triggers a cloud function that calls the API and downloads data , processes, uploads new rows to BigQuery. All within free limits so the cost is under £1 a month based on 1 run a day.

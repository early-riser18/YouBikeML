# YouBike 🚲
This project aims at improving the fleet rebalancing of YouBikes in Taiwan.
Currently, it provides a service to predict the number of bikes available at any YouBike station in Taiwan in the next three hours.
**Try it out on [Streamlit](https://smarter-youbike.streamlit.app/) now!**

## What is the problem?
YouBike users who wish to get a bike to travel around the city are sometimes not able to find any bike at the station surrounding them. Therefore they have to rely on other methods of transportation, which might be more expensive, slower or less convenient. 

## How we are solving it
As a first step, we focus on forecasting the number of YouBikes available at any stations in the next three hours.
From there, we aim to help the Youbike team make more informed decisions on which stations are most likely to run out of bikes, and therefore need to be resupplied.

Youbike stations' current state is ingested every 30 minutes, transformed and clean to form a standardized layer for downstream services.

*Disclaimer: This project is first a data engineering project. The current model is naive and does not product good results. It will be improved in the future.*
# How it works
![Architecture Diagram](https://private-user-images.githubusercontent.com/65651754/326742006-a69652fc-1486-4235-8955-1f18927f3b8a.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3MTQ0Njg4MjgsIm5iZiI6MTcxNDQ2ODUyOCwicGF0aCI6Ii82NTY1MTc1NC8zMjY3NDIwMDYtYTY5NjUyZmMtMTQ4Ni00MjM1LTg5NTUtMWYxODkyN2YzYjhhLnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNDA0MzAlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjQwNDMwVDA5MTUyOFomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPTZiNjBhZWU2N2Q0YzU0MzQ3MThkZjgwMTZkYTkyNTU1MjlmMzFjMDEzZTI2ODg3MDc0MTRiMzc0NjU1OTIyODYmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0JmFjdG9yX2lkPTAma2V5X2lkPTAmcmVwb19pZD0wIn0.aA5Qmn87QzYu5-wQSsZlf_mU40zLAaEuc7picqUlJRU)
A predictive timeseries model is trained using real-time, publicly accessible data on the quantity of bikes docked at YouBike stations, as well as weather data.
## Requesting a forecast
The predictive model is hosted on the cloud and queried via API. When requesting to forecast the number of bikes available at the Youbike stations, the real-time state of all Youbike stations is retrieved and combined with a forecast of the day's weather.
<br><br>**Try it out**
```bash
curl "https://33iqhftc6fakzdlczkjsiikhtu0odrec.lambda-url.ap-northeast-1.on.aws/" -H 'content-type: application/json' -d '{"service": "fill_rate_forecast", "station_id": [508201032, 501208101]}'
```
Note: More station ids can be found by querying the other available service.
```bash
curl "https://33iqhftc6fakzdlczkjsiikhtu0odrec.lambda-url.ap-northeast-1.on.aws/" -H 'content-type: application/json' -d '{"service": "bike_station_status", "extended": 1}'
```

## Training the model
The model is retrained periodically based on the latest historical data. 

### Data sources
YouBike Stations data is obtained via [YouBike Official Website](https://www.youbike.com.tw/region/main/stations/)<br>
Weather data is obtained thanks to the [Open Meteo API](https://open-meteo.com/)<br>
 
# Set up project
## Locally
### Set up your environment
```bash
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
export AWS_PROFILE=""
export DATABASE_URL="" # Find it on your CockroachDB account. Looks like: cockroachdb://<account-name>:<password>@<project-name>-6569.6xw.<cloud-region>.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full

export APP_ENV=local #replace by 'stage' to run it on the cloud-based stage env.

export MINIO_ACCESS_KEY_ID=minio-local
export MINIO_SECRET_ACCESS_KEY=minio-local
export MINIO_HOST=localhost
```
Install the python dependencies via
```bash
pip install -r requirements.txt
```
Make sure the dependencies are available on your path
## Cloud Environment
You will need the following accounts: Terraform, Prefect Cloud, AWS
1. #TODO clarify how to have credentials locally
2. Run `terraform apply` to spin up the S3 bucket, VPC, ECR and ECS Cluster
4. Build an image with the Dockerfile and push it to the newly-created ECR
3. Create a Prefect Work Pool of type ECS:Push by passing the infrastructure ids generated ([see source documentation](https://docs.prefect.io/latest/guides/deployment/push-work-pools/#manual-infrastructure-provisioning))
4. Deploy the flows to your Prefect Server by running `python3 -m flows.<flowname>

# Extraction
## Locally
A local S3 bucket is available via MinIO.
1. Spin up a local MinIO instance with `docker-compose up minio-server`
2. Set the following ENV variables
```bash
export MINIO_ACCESS_KEY_ID=minio-local
export MINIO_SECRET_ACCESS_KEY=minio-local
export MINIO_HOST=localhost
```
3. Run any extraction flow via `python3 -m flows.<flow-name>
4. Inspect the extracted data on `http://localhost:9001` using the credentials set in the previous step.
## Cloud Environment
1. Go to your Prefect Server > Deployments 
2. Select Quick Run on any deployment of interest. 
3. Inspect the extracted data on your S3 bucket.

# Infrastructure
- main components of architecture 
- how to set it up yourself 

This project uses AWS for multiple cloud services. The data lake is hosted on S3, data ingestion and transformation tasks are run on ECS instances and scheduled with Prefect push Work Pools.

Services within the AWS ecosystem communicate via a VPC with a public subnet. 
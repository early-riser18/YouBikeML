# YouBike
This project aims at improving the fleet rebalancing of YouBikes in Taiwan.

## What is the problem?
YouBike users who wish to get a bike to travel around the city are sometimes not able to find any bike at the station surrounding them. Therefore they have to rely on other methods of transportation, which might be more expensive, slower or less convenient. 


## How we are solving it
As a first step, we plan to forecast the average occupancy of youbike stations between now and a few hours in the future (exact duration can be adjusted).
From there, we aim to help the Youbike team make more informed decisions on which station is most likely to run out of bikes, and therefore need to be resupplied.

# Set up project
## Locally
Use the ENV variable `APP_ENV: local` to run the project locally via your CLI.
Install the python dependencies via
```bash
pip install -r requirements.txt
```
Make sure your dependencies are available on your path
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
2. Go to your Prefect Server > Deployments 
3. Select Quick Run on any deployment of interest. 
4. Inspect the extracted data on your S3 bucket.

# Infrastructure
- main components of architecture 
- how to set it up yourself 

This project uses AWS for multiple cloud services. The data lake is hosted on S3, data ingestion and transformation tasks are run on ECS instances and scheduled with Prefect push Work Pools.

Services within the AWS ecosystem communicate via a VPC with a public subnet. 
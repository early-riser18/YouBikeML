FROM python:3.10.13-slim-bullseye as python3.10-prefect
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python3.10-prefect
RUN pip install awslambdaric
WORKDIR /opt/prefect
COPY ./utils ./utils
COPY ./etl ./etl
COPY ./db ./db
COPY ./etl/flows ./flows
COPY ./api ./api 
COPY ./predict ./predict 

ENV PREFECT_LOGGING_LEVEL=DEBUG
ENV DATABASE_URL="cockroachdb://default:PFdaW6lkaehksaBj3@youbike-6585.6xw.aws-ap-southeast-1.cockroachlabs.cloud:26257/defaultdb"
ENV TZ="Asia/Taipei"

CMD BASH
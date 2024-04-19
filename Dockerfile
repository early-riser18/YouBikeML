FROM python:3.10.13-slim-bullseye as python3.10-prefect
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python3.10-prefect
RUN pip install awslambdaric
WORKDIR /opt/prefect
COPY ./utils ./utils
COPY ./flows ./flows
COPY ./extraction ./extraction
COPY ./transform ./transform
COPY ./predict ./predict
COPY ./db ./db

ENV MINIO_ACCESS_KEY_ID=minio-local
ENV MINIO_SECRET_ACCESS_KEY=minio-local
ENV MINIO_HOST=minio-server
ENV PREFECT_LOGGING_LEVEL=DEBUG
CMD bash
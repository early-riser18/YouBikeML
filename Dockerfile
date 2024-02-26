FROM prefecthq/prefect:2.14-python3.9 as python3.9-prefect

COPY ./requirements.txt .
RUN pip install -r requirements.txt

FROM python3.9-prefect
COPY ./utils /opt/prefect/utils
COPY ./flows /opt/prefect/flows

ENV MINIO_ACCESS_KEY_ID=minio-local
ENV MINIO_SECRET_ACCESS_KEY=minio-local
ENV MINIO_HOST=minio-server

CMD  python3 -m flows.${FLOW_NAME}
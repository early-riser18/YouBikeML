build:
	docker-compose build

run-local: build	
	@APP_ENV=local; \
    export APP_ENV; \
    docker-compose up --scale spark-worker=3
	
run-stage: build
	@APP_ENV=stage; \
    export APP_ENV; \
    docker-compose up --scale spark-worker=3
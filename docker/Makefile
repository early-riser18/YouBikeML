build:
	docker-compose down --remove-orphans; \
	docker-compose build

run-local: 
	@APP_ENV=local; \
    export APP_ENV; \
    docker-compose up --scale spark-worker=3
	
run-stage: 
	@APP_ENV=stage; \
    export APP_ENV; \
    docker-compose up --scale spark-worker=3
#!/bin/bash
set -eo pipefail
IFS=$'\n\t'

#Check if script is run from root
if [ -z $(echo $PWD | grep '/youbike$') ]; then
    echo "This script must be run from repository's root dir."
    exit 1
fi

IMG_NAME=prefect-flows
REPO="211125707335.dkr.ecr.ap-northeast-1.amazonaws.com"
LOCAL_ARCH=linux/arm64
CLOUD_ARCH=linux/amd64

build_image() {
    echo "Building image: $IMG_NAME:$TAG for $SYS_ARCH" 
    docker build --platform $SYS_ARCH -t $IMG_NAME:$TAG -f ${PWD}/docker/backend/Dockerfile ${PWD}

}

echo "Select an option:"
names=("Test locally" "Test lambda" "Deploy image")

select name in "${names[@]}" ; do
    if [[ -n $name ]]; then
        selected=("$name") 
        break
    else
        echo "Invalid option. Try another one."
    fi
done

echo "Setting env to local"
export APP_ENV=local

case ${selected[@]} in
    "Test locally")
        SYS_ARCH=$LOCAL_ARCH
        TAG=latest-local
        build_image
        docker-compose -f ${PWD}/docker/compose.yaml up -d minio-server 
        docker-compose -f ${PWD}/docker/compose.yaml run -it backend
    ;;
    "Test lambda")
        SYS_ARCH=$LOCAL_ARCH
        TAG=latest-local
        build_image
        docker-compose -f ${PWD}/docker/compose.yaml run -d -v ~/.aws-lambda-rie:/aws-lambda -p 9999:8080 \
        --entrypoint /aws-lambda/aws-lambda-rie \
        backend \
        /usr/local/bin/python -m awslambdaric api.lambda_handler.lambda_handler         
        echo "Local lambda function running and available at \"http://localhost:9999/2015-03-31/functions/function/invocations\" "
        docker-compose -f ${PWD}/docker/compose.yaml up -d minio-server 
    ;;
    "Deploy image")
        SYS_ARCH=$CLOUD_ARCH
        TAG=latest
        build_image
        echo "Pushing to repo $REPO/$IMG_NAME as $TAG" 
        aws ecr get-login-password --region ap-northeast-1 | docker login --username AWS --password-stdin 211125707335.dkr.ecr.ap-northeast-1.amazonaws.com
	    docker tag $IMG_NAME:$TAG $REPO/$IMG_NAME:$TAG
        docker push $REPO/$IMG_NAME:$TAG
        
        echo "Updating Lambda Function"
        aws lambda update-function-code \
        --function-name get-youbike-forecast \
        --image-uri 211125707335.dkr.ecr.ap-northeast-1.amazonaws.com/prefect-flows:latest
    ;;
esac
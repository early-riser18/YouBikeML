#!/bin/zsh
set -eo pipefail
IFS=$'\n\t'

IMG_NAME=prefect-flows
DEFAULT_TAG=latest
LOCAL_TAG=latest-local

REPO="211125707335.dkr.ecr.ap-northeast-1.amazonaws.com"


echo "Building img=$IMG_NAME:$TAG" 
if [[ -n $1 ]] && [[ $1 != x ]]; then

	echo "Build for local run? (y/n)"
	read local_run
	if [[ $local_run == "y" ]]; then
		docker build -t $IMG_NAME:$LOCAL_TAG .
		TAG=$LOCAL_TAG
	
	elif [[ $local_run == "n" ]]; then
		docker build --platform=linux/amd64 -t $IMG_NAME:$DEFAULT_TAG .
		TAG=$DEFAULT_TAG
	fi 
fi

echo $TAG
if [[ -n $2 ]]; then
	if [[ $TAG == $LOCAL_TAG ]]; then
		echo "you are going to push a different version than local, better rebuild with default tag"
		exit 1
	fi
	echo "Pushing to repo $REPO/$IMG_NAME as $TAG" 
	aws ecr get-login-password --region ap-northeast-1 | docker login --username AWS --password-stdin 211125707335.dkr.ecr.ap-northeast-1.amazonaws.com
	docker tag $IMG_NAME:$TAG $REPO/$IMG_NAME:$TAG
	# docker run -e FLOW_NAME=youbike -it $IMG_NAME:$TAG 
	docker push $REPO/$IMG_NAME:$TAG

	aws lambda update-function-code \
           --function-name get-youbike-forecast \
           --image-uri 211125707335.dkr.ecr.ap-northeast-1.amazonaws.com/prefect-flows:latest
fi 
#!/bin/bash
set -eox pipefail
IFS=$'\n\t'

IMG_NAME=prefect-flows
TAG=latest
REPO="211125707335.dkr.ecr.ap-northeast-1.amazonaws.com"

echo "Building img=$IMG_NAME:$TAG" 
if [[ -n $1 ]] && [[ $1 != x ]]; then

	docker build -t $IMG_NAME:$TAG .
fi 

if [[ -n $2 ]]; then
	echo "Pushing to repo $REPO/$IMG_NAME as $TAG" 
	aws ecr get-login-password --region ap-northeast-1 | docker login --username AWS --password-stdin 211125707335.dkr.ecr.ap-northeast-1.amazonaws.com
	docker tag $IMG_NAME:$TAG $REPO/$IMG_NAME:$TAG
	# docker run -e FLOW_NAME=youbike -it $IMG_NAME:$TAG 
	docker push $REPO/$IMG_NAME:$TAG
fi 
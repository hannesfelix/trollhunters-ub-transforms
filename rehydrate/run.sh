#!/bin/sh

TABLE=$1
PERCENTAGE=$2

docker run --name rehydrate -d \
	--env-file="./.env" \
	--net nett \
	-p 4042:4040 \
	nandanrao/rehydrate \
	python rehydrate.py $TABLE "gs://spain-tweets/rehydrated/lake/month=${TABLE}" $PERCENTAGE

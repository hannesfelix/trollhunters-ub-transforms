#!/bin/sh

TABLE=$1
MONTH=$2

docker run --name writetable --rm \
	-v /extracts/parquet:/home/jovyan/outs \
	--net nett \
	-p 4041:4040 \
	nandanrao/writetable \
	python write_table.py $TABLE "gs://spain-tweets/ub-originals/month=${MONTH}"

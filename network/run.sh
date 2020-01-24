#!/bin/sh

MONTH=$1
TYPE=$2
OUTFILE=$3

docker run --name network -d \
	--net nett \
	-p 4041:4040 \
	nandanrao/network \
	python network.py $MONTH $TYPE $OUTFILE

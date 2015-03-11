#!/bin/bash

source ./index_env.sh

python dosample.py \
--shards=300 \
--splitfile=$SPLIT_FILE \
--conf-path ./mrjob.conf \
--cmdenv AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
--cmdenv AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
-r emr $WARC_CDX &> /tmp/emrrun.log &


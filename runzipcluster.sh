#!/bin/bash

source ./index_env.sh

python zipnumclusterjob.py \
--shards=300 \
--splitfile=$SPLIT_FILE \
--output-dir="$ZIPNUM_CLUSTER_DIR" \
--no-output \
--conf-path ./mrjob.conf \
--cmdenv AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
--cmdenv AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
-r emr $WARC_CDX &> /tmp/emrrun2.log &


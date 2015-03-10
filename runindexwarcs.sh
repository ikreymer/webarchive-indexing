#!/bin/bash
#input="s3://cc-cdx-index/paths/dec2014.warcpaths.shuf.txt"
input="s3://cc-cdx-index/paths/jan2015.warcs.txt"

source ./s3env.sh

python indexwarcsjob.py \
--conf-path ./s3env.conf \
--cdx_bucket=cc-cdx-index \
--cmdenv AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
--cmdenv AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
-r emr $input &> /tmp/emrrun.log &


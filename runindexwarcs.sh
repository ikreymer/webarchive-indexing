#!/bin/bash
input="s3://cc-cdx-index/paths/dec2014.warcpaths.shuf.txt"
#input="s3://cc-cdx-index/paths/dec2014-try2.txt"
source ./env.sh
python indexwarcs.py --conf-path ./mrjob.conf --cmdenv AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID --cmdenv AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -r emr $input &> /tmp/emrrun.log &


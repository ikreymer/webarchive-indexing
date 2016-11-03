#!/bin/bash

if [ $# -lt 2 ]; then
    cat <<"EOF"
$0 <year-week-of-crawl> <path-to-warc-file-list> [<split_file>]

Create a Common Crawl index for a monthly crawl. All steps are run on Hadoop.

  <year-week-of-crawl>   year and week of the monthly crawl to be indexed, e.g. 2016-44
                         used to determine location of the index
                             s3://commoncrawl/cc-index/collections/CC-MAIN-2016-44/...

  <path-to-warc-file-list>  list of WARC file objects to be indexed, e.g, the WARC list
                               s3://commoncrawl/crawl-data/CC-MAIN-2016-44/warc.paths.gz
                         Paths in the list must be keys/objects in the Common Crawl bucket.
                         The path to the list must be a valid and complete HDFS or S3A URL,
                         e.g. hdfs://hdfs-master.example.com/user/hadoop-user/CC-MAIN-2016-44.paths
                         The "index warcs" step is skipped if an empty string is passed as argument.

  <split_file>           Optional split file to be reused from previous crawl with similar distribution of URLs.
                         If not given, splits are calculated and saved on the default split file path.

Environment variables depend upon:
  AWS_ACCESS_KEY_ID      - AWS credentials used by Boto to access the bucket (read and write)
  AWS_SECRET_ACCESS_KEY
EOF
    exit 1
fi

if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "AWS credentials must passed to Boto via environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY!"
    exit 1
fi


YEARWEEK="$1"
WARC_MANIFEST="$2"
REUSE_SPLIT_FILE="$3"

echo "Generating cc-index for $YEARWEEK"
echo
echo WARC_MANIFEST="$WARC_MANIFEST"
echo

export WARC_CDX="s3a://commoncrawl/cc-index/cdx/CC-MAIN-$YEARWEEK/segments/*/*/*.cdx.gz"

export WARC_CDX_BUCKET="commoncrawl"

export ZIPNUM_CLUSTER_DIR="s3a://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/indexes/"

# SPLIT_FILE could be reused from previous crawl with similar distribution of URLs, see REUSE_SPLIT_FILE
export SPLIT_FILE="s3a://cc-cdx-index/${YEARWEEK}_splits.seq"


export LC_ALL=C

set -e
set -x


if [ -n "$WARC_MANIFEST" ]; then
    python indexwarcsjob.py \
           --cdx_bucket=$WARC_CDX_BUCKET \
       --no-output \
       --cleanup NONE \
       --skip-existing \
       --cmdenv AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
       --cmdenv AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
       -r hadoop \
       --jobconf "mapreduce.map.memory.mb=800" \
       --jobconf "mapreduce.map.java.opts=-Xmx512m" \
       $WARC_MANIFEST
fi


if [ -n "$REUSE_SPLIT_FILE" ]; then
    SPLIT_FILE="$REUSE_SPLIT_FILE"
else
    # mapreduce.output.fileoutputformat.compress=true
    #    must compress task output to avoid that the single reducer node fails with a full disk
    #    anyway, it may require 60 GB of local disk space on the reducer node
    # mapreduce.map.memory.mb=640
    #    mappers read only small cdx files: minimal memory requirements
    python dosample.py \
           --verbose \
           --shards=300 \
           --splitfile=$SPLIT_FILE \
           --cmdenv AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
           --cmdenv AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
           --jobconf "mapreduce.map.memory.mb=640" \
           --jobconf "mapreduce.map.java.opts=-Xmx512m" \
           --jobconf "mapreduce.reduce.memory.mb=1024" \
           --jobconf "mapreduce.reduce.java.opts=-Xmx512m" \
           --jobconf "mapreduce.output.fileoutputformat.compress=true" \
           -r hadoop $WARC_CDX
    mv splits.seq $(basename s3${SPLIT_FILE#s3a})

    if s3cmd info s3${SPLIT_FILE#s3a}; then
        echo "Ok, split file was upload"
    else
        echo "Uploading split file ..."
        s3cmd put $(basename s3${SPLIT_FILE#s3a}) s3${SPLIT_FILE#s3a}
    fi
fi


python zipnumclusterjob.py \
       --shards=300 \
       --splitfile=$SPLIT_FILE \
       --output-dir="$ZIPNUM_CLUSTER_DIR" \
       --no-output \
       --cmdenv AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
       --cmdenv AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
       --jobconf "mapreduce.map.memory.mb=640" \
       --jobconf "mapreduce.map.java.opts=-Xmx512m" \
       --jobconf "mapreduce.reduce.memory.mb=1536" \
       --jobconf "mapreduce.reduce.java.opts=-Xmx1024m" \
       -r hadoop $WARC_CDX


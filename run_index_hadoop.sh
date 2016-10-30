#!/bin/bash

if [ $# -ne 2 ]; then
    cat <<"EOF"
$0 <year-week-of-crawl> <path-to-warc-file-list>

Create a Common Crawl index for a monthly crawl. All steps are run on Hadoop.

  <year-week-of-crawl>   year and week of the monthly crawl to be indexed, e.g. 2016-44
                         used to determine location of the index
                             s3://commoncrawl/cc-index/collections/CC-MAIN-2016-44/...

  <path-to-warc-file-list>  list of WARC file objects to be indexed, e.g, the WARC list
                               s3://commoncrawl/crawl-data/CC-MAIN-2016-44/warc.paths.gz
                            Paths in the list must be keys/objects in the Common Crawl bucket.
                            The path to the list must be a valid and complete HDFS or S3A URL,
                            e.g. hdfs://hdfs-master.example.com/user/hadoop-user/CC-MAIN-2016-44.paths

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

echo "Generating cc-index for $YEARWEEK"
echo
echo WARC_MANIFEST="$WARC_MANIFEST"
echo

export WARC_CDX="s3a://commoncrawl/cc-index/cdx/CC-MAIN-$YEARWEEK/segments/*/*/*.cdx.gz"

export WARC_CDX_BUCKET="commoncrawl"

export ZIPNUM_CLUSTER_DIR="s3a://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/indexes/"

# SPLIT_FILE could be reused from previous crawl with similar distribution of URLs
export SPLIT_FILE="s3a://cc-cdx-index/${YEARWEEK}_splits.seq"


set -e
set -x


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


python dosample.py \
       --verbose \
       --shards=300 \
       --splitfile=$SPLIT_FILE \
       --cmdenv AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
       --cmdenv AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
       --jobconf "mapreduce.map.memory.mb=1024" \
       --jobconf "mapreduce.map.java.opts=-Xmx512m" \
       --jobconf "mapreduce.reduce.memory.mb=1024" \
       --jobconf "mapreduce.reduce.java.opts=-Xmx512m" \
       -r hadoop $WARC_CDX
mv splits.seq $(basename s3${SPLIT_FILE#s3a})

if s3cmd info s3${SPLIT_FILE#s3a}; then
    echo "Ok, split file was upload"
else
    echo "Uploading split file ..."
    s3cmd put $(basename s3${SPLIT_FILE#s3a}) s3${SPLIT_FILE#s3a}
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


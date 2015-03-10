input="s3://cc-cdx-index/cdx/CC-MAIN-2014-52/segments/1418802764809.9/*/*.cdx.gz"
#input="s3://cc-cdx-index/cdx/CC-MAIN-2014-52/segments/*/*/*.cdx.gz"
output="s3://cc-cdx-index/dec2014/test2/"

source ./s3env.sh
python zipnumclusterjob.py \
--shards=10 \
--splitfile=s3://cc-cdx-index/dec2014/splitstest.seq \
--output-dir="$output" \
--no-output \
--conf-path ./mrjob.conf \
-r emr $input &> /tmp/emrrun.log &


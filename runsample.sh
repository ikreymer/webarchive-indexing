input="s3://cc-cdx-index/cdx/CC-MAIN-2014-52/segments/1418802764809.9/*/*.cdx.gz"
#input="s3://cc-cdx-index/cdx/CC-MAIN-2014-52/segments/*/*/*.cdx.gz"
#output="s3://cc-cdx-index/dec2014/splits/"

source ./s3env.sh

python runsample.py \
--shards=10 \
--conf-path ./mrjob.conf \
-r emr $input &> /tmp/emrrun.log &


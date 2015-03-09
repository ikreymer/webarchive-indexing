input="s3://cc-cdx-index/cdx/CC-MAIN-2014-52/segments/*/*/*.cdx.gz"
#input="s3://cc-cdx-index/cdx/CC-MAIN-2014-52/segments/1418802764809.9/*/*.cdx.gz"
output="s3://cc-cdx-index/dec2014/zipnum2/"

source ./env.sh
python zipnumwriter.py --conf-path ./mrjob.conf --cmdenv AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID --cmdenv AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -r emr $input --no-output --output-dir="$output" &> /tmp/emrrun.log &


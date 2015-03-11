from samplecdxjob import SampleCDXJob
from seqfileutils import make_text_null_seq

import sys
import tempfile
import os
import shutil

SEQ_FILE = 'splits.seq'

def run_sample_job():
    job = SampleCDXJob(args=sys.argv[1:])

    with job.make_runner() as runner:
        runner.run()

        if os.path.isfile(SEQ_FILE):
            shutil.remove(SEQ_FILE)

        # convert streaming output to sequence file
        count = make_text_null_seq(SEQ_FILE, runner.stream_output())

    if job.options.splitfile and hasattr(runner.fs, 'make_s3_key'):
        key = job.fs.make_s3_key(job.options.splitfile)
        key.set_contents_from_filename(SEQ_FILE)

def main():
    run_sample_job()


if __name__ == "__main__":
    main()

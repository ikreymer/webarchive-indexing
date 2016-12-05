from samplecdxjob import SampleCDXJob
from seqfileutils import make_text_null_seq

import sys
import tempfile
import os

SEQ_FILE = 'splits.seq'
SPL_FILE = 'splits.txt'

def run_sample_job():
    job = SampleCDXJob(args=sys.argv[1:])

    with job.make_runner() as runner:
        runner.run()

        if os.path.isfile(SEQ_FILE):
            os.remove(SEQ_FILE)
        if os.path.isfile(SPL_FILE):
            os.remove(SPL_FILE)

        # dump streaming output to file
        with open(SPL_FILE, 'wb') as fh:
            for x in runner.stream_output():
                fh.write(x)
            fh.close()

        # convert streaming output to sequence file
        count = make_text_null_seq(SEQ_FILE, open(SPL_FILE))

    if job.options.splitfile and hasattr(runner.fs, 'make_s3_key'):
        key = job.fs.make_s3_key(job.options.splitfile)
        key.set_contents_from_filename(SEQ_FILE)

def main():
    run_sample_job()


if __name__ == "__main__":
    main()

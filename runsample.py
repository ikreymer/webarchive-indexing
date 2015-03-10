from samplecdxjob import SampleCDXJob
from seqfileutils import make_text_null_seq

import sys
import tempfile
import os

SEQ_FILE = 'splits.seq'

def main():
    job = SampleCDXJob(args=sys.argv[1:])

    with job.make_runner() as runner:
        runner.run()

        if os.path.isfile(SEQ_FILE):
            shutil.remove(SEQ_FILE)
        # convert streaming output to sequence file
        make_text_null_seq(SEQ_FILE, runner.stream_output())


if __name__ == "__main__":
    main()

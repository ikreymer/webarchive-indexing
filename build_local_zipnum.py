import os
import glob
from argparse import ArgumentParser
from zipnumclusterjob import ZipNumClusterJob
from mrjob.launch import MRJobLauncher
import logging
import sys


log = logging.getLogger(__name__)


#=============================================================================
def run_job(input_path, output_dir, shards, parallel, lines=None):
    args = ['--no-output', '--output-dir', output_dir, '-r']
    if parallel:
        args.append('local')
    else:
        args.append('inline')

    args.append('--shards=' + str(shards))

    if lines:
        args.append('--numlines=' + str(lines))

    if isinstance(input_path, list):
        args.extend(input_path)
    else:
        args.append(input_path)

    output_dir = os.path.abspath(output_dir)
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    os.environ['mapreduce_output_fileoutputformat_outputdir'] = output_dir

    job = ZipNumClusterJob(args)

    with job.make_runner() as runner:
        runner.run()


def build_summary_and_loc(output_dir):
    # Write summary file
    full = os.path.join(output_dir, 'part-*')

    inputs = sorted(glob.glob(full))

    summary_file = os.path.join(output_dir, 'cluster.summary')
    print('Building Summary File: ' + summary_file)

    count = 1
    with open(summary_file, 'w+b') as fh:
        for filein in inputs:
            with open(filein, 'r+b') as partfh:
                for line in partfh:
                    line = line.rstrip()
                    line += '\t' + str(count)
                    fh.write(line + '\n')
                    count += 1

    # Write loc file
    full = os.path.join(output_dir, 'cdx-*')

    inputs = sorted(glob.glob(full))

    loc_file = os.path.join(output_dir, 'cluster.loc')
    print('Building Loc File: ' + loc_file)
    with open(loc_file, 'w+b') as fh:
        for filename in inputs:
            fh.write(os.path.basename(filename) + '\t' + filename + '\n')


def main():
    parser = ArgumentParser()
    parser.add_argument('output', help='ZipNum Cluster Output directory')
    parser.add_argument('inputs', nargs='+', help='CDX Input glob eg: /cdx/*.cdx.gz')
    parser.add_argument('-s', '--shards', default=10, type=int,
                        help='Number of ZipNum Cluster shards to create')

    parser.add_argument('-l', '--numlines', default=3000, type=int,
                        help='Number of lines per gzip block (default 3000)')

    parser.add_argument('-p', '--parallel', action='store_true',
                        help='Run in parllel (multiple maps/reducer processes)')

    r = parser.parse_args()

    MRJobLauncher.set_up_logging(quiet=False,
                                 verbose=False,
                                 stream=sys.stderr)

    log.setLevel(logging.INFO)
    compat_log = logging.getLogger('mrjob.compat')
    compat_log.setLevel(logging.ERROR)

    run_job(r.inputs, r.output, r.shards, r.parallel, r.numlines)
    build_summary_and_loc(r.output)

if __name__ == "__main__":
    main()

import boto
import shutil
import sys

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

from tempfile import TemporaryFile
from pywb.warc.cdxindexer import write_cdx_index
from gzip import GzipFile


#=============================================================================
class IndexWARCJob(MRJob):
    """ This job receives as input a manifest of WARC/ARC files and produces
    a CDX index per file

    The pywb.warc.cdxindexer is used to create the index, with a fixed set of options
    TODO: add way to customized indexing options.

    """
    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'

    JOBCONF =  {'mapreduce.task.timeout': '9600000',
                'mapreduce.input.fileinputformat.split.maxsize': '50000000',
                'mapreduce.map.speculative': 'false',
                'mapreduce.reduce.speculative': 'false',
                'mapreduce.job.jvm.numtasks': '-1',

                'mapreduce.input.lineinputformat.linespermap': 2,
                }

    def configure_options(self):
        """Custom command line options for indexing"""
        super(IndexWARCJob, self).configure_options()

        self.add_passthrough_option('--warc_bucket', dest='warc_bucket',
                                    default='aws-publicdatasets',
                                    help='source bucket for warc paths, if input is a relative path (S3 Only)')

        self.add_passthrough_option('--cdx_bucket', dest='cdx_bucket',
                                    default='my_cdx_bucket',
                                    help='destination bucket for cdx (S3 Only)')

        self.add_passthrough_option('--skip-existing', dest='skip_existing', action='store_true',
                                    help='skip processing files that already have CDX',
                                    default=True)

    def mapper_init(self):
        # Note: this assumes that credentials are set via
        # AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables
        self.conn = boto.connect_s3()

        self.warc_bucket = self.conn.lookup(self.options.warc_bucket)
        assert(self.warc_bucket)

        self.cdx_bucket = self.conn.lookup(self.options.cdx_bucket)
        assert(self.cdx_bucket)

        self.index_options = {
            'surt_ordered': True,
            'sort': True,
            'cdxj': True,
            'minimal': True
        }

    def mapper(self, _, line):
        warc_path = line.split('\t')[-1]
        try:
            self._load_and_index(warc_path)
        except Exception as exc:
            sys.stderr.write(warc_path + '\n')
            raise

    def _conv_warc_to_cdx_path(self, warc_path):
        # set cdx path
        cdx_path = warc_path.replace('common-crawl/crawl-data', 'cdx2')
        cdx_path = cdx_path.replace('.warc.gz', '.cdx.gz')
        return cdx_path

    def _load_and_index(self, warc_path):
        warckey = self.warc_bucket.get_key(warc_path)

        cdx_path = self._conv_warc_to_cdx_path(warc_path)

        if self.options.skip_existing:
            cdxkey = self.cdx_bucket.get_key(cdx_path)

            if cdxkey:
                sys.stderr.write('Already Exists\n')
                return

        with TemporaryFile(mode='w+b') as warctemp:
            shutil.copyfileobj(warckey, warctemp)

            warctemp.seek(0)

            with TemporaryFile(mode='w+b') as cdxtemp:
                with GzipFile(fileobj=cdxtemp, mode='w+b') as cdxfile:
                    # Index to temp
                    write_cdx_index(cdxfile, warctemp, warc_path, **self.index_options)

                # Upload temp
                cdxkey = self.cdx_bucket.new_key(cdx_path)
                cdxtemp.flush()

                cdxkey.set_contents_from_file(cdxtemp, rewind=True)


if __name__ == "__main__":
    IndexWARCJob.run()

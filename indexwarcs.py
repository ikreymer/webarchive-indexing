import boto
import shutil

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

from tempfile import TemporaryFile
from pywb.warc.cdxindexer import write_cdx_index
from gzip import GzipFile


class IndexWARCJob(MRJob):
    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'
    JOBCONF = {'mapred.line.input.format.linespermap': 1}

    def mapper_init(self):
        self.conn = boto.connect_s3()
        self.warc_bucket = self.conn.lookup('aws-publicdatasets')
        assert(self.warc_bucket)

        self.cdx_bucket = self.conn.lookup('warcwork')
        assert(self.cdx_bucket)

        self.cdx_path = '/cdx/dec2014/'

        self.index_options = {
            'surt_ordered': True,
            'sort': True,
#            'append_post': True,
            'cdx06': True,
            'minimal': True
        }

    def mapper(self, _, line):
        filepath = line.split('\t')[-1]
        try:
            self._load_and_index(filepath)
        except Exception as exc:
            import traceback
            err_details = filepath + '\n'
            err_details += traceback.format_exc(exc)
            yield '', err_details

    def _load_and_index(self, filepath):
        warckey = self.warc_bucket.get_key(filepath)

        with TemporaryFile(mode='w+b') as warctemp:
            shutil.copyfileobj(warckey, warctemp)
            warctemp.seek(0)

            with TemporaryFile(mode='w+b') as cdxtemp:
                cdxfile = GzipFile(fileobj=cdxtemp, mode='w+b')

                # Index to temp
                filename = filepath.rsplit('/')[-1]
                write_cdx_index(cdxfile, warctemp, filename, **self.index_options)

                # Upload temp
                path = self.cdx_path + filename.replace('.warc.gz', '.cdx.gz')
                cdxkey = self.cdx_bucket.new_key(path)
                cdxkey.set_contents_from_file(cdxtemp, rewind=True)


if __name__ == "__main__":
    IndexWARCJob.run()

import boto
import shutil
import sys

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

from tempfile import TemporaryFile
from pywb.warc.cdxindexer import write_cdx_index
from gzip import GzipFile

#boto.config.add_section('Boto')
#boto.config.set('Boto','http_socket_timeout', '400')

class IndexWARCJob(MRJob):
    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'
    JOBCONF = {'mapred.line.input.format.linespermap': 1}

    def mapper_init(self):
        self.conn = boto.connect_s3()
        self.warc_bucket = self.conn.lookup('aws-publicdatasets')
        assert(self.warc_bucket)

        self.cdx_bucket = self.conn.lookup('cc-cdx-index')
        assert(self.cdx_bucket)

        #self.cdx_path = '/cdx/'

        self.index_options = {
            'surt_ordered': True,
            'sort': True,
#            'append_post': True,
            'cdx06': True,
            'minimal': True
        }

    def mapper(self, _, line):
        warc_path = line.split('\t')[-1]
        try:
            self._load_and_index(warc_path)
        except DoRetryException as rt:
            return 1
        except Exception as exc:
            import traceback
            err_details = warc_path + '\n'
            err_details += traceback.format_exc(exc)
            yield '', err_details

    def _load_and_index(self, warc_path):
        warckey = self.warc_bucket.get_key(warc_path)

        # set cdx path
        cdx_path = warc_path.replace('common-crawl/crawl-data', '/cdx')
        cdx_path = cdx_path.replace('.warc.gz', '.cdx.gz')
        cdxkey = self.cdx_bucket.get_key(cdx_path)
        # already exists, skipping
        if cdxkey:
            sys.stderr.write('Already Exists\n')
            return

        with TemporaryFile(mode='w+b') as warctemp:
            try:
                shutil.copyfileobj(warckey, warctemp)
            except:
                raise DoRetryException()

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

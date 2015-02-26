from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol


class IndexWARCJob(MRJob):
    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'
    JOBCONF = {'mapred.line.input.format.linespermap': 1}

    def mapper_init(self):
        super(IndexWARCJob, self).mapper_init()

        self.conn = boto.connect_s3()
        self.warc_bucket = self.conn.lookup('aws-publicdatasets')

        self.cdx_bucket = self.conn.lookup('cc-cdx')
        self.cdx_path = '/cdx/dec2014/'

        self.index_options = {
            'sort': True,
            'append_post': True,
            'cdx06': True,
            'minimal': True
        }

    def mapper(self, _, line):
        filepath = line.split('\t')[-1]
        try:
            self._load_and_index(filepath)
        except:
            yield '', filepath

    def _load_and_index(self, filepath):
        warckey = self.warc_bucket.get_key(filepath)

        with TemporaryFile(mode='w+b') as cdxtemp:
            # Index to temp
            write_cdx_index(cdxtemp, warckey, filename, self.index_options)

            # Upload temp
            path = self.cdx_path + filepath.rsplit('/')[-1]
            cdxkey = self.cdx_bucket.new_key(path)
            cdxkey.set_contents_from_file(cdxtemp, rewind=True)

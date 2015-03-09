import boto
import shutil
import sys
import os

import zlib
import urlparse
import json

from tempfile import TemporaryFile

from mrjob.job import MRJob
from mrjob.protocol import RawProtocol, RawValueProtocol

NUM_LINES = 3000

# must be set to splits file
PART_FILE = 's3://path/to/splits_file'

class ZipNumWriterJob(MRJob):
    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.CombineTextInputFormat'
    #HADOOP_OUTPUT_FORMAT = 'org.apache.hadoop.mapred.SequenceFileOutputFormat'

    PARTITIONER = 'org.apache.hadoop.mapred.lib.TotalOrderPartitioner'

    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol
    INTERNAL_PROTOCOL = RawProtocol

    JOBCONF = {'mapreduce.job.reduces': '302',
               'mapreduce.input.fileinputformat.split.maxsize': '50000000',
               'mapreduce.map.speculative': 'false',
               'mapreduce.job.jvm.numtasks': '10',
               'mapreduce.reduce.speculative': 'false',

               'mapreduce.totalorderpartitioner.path': PART_FILE}

    def mapper_init(self):
        pass

    def mapper(self, _, line):
        line = line.split('\t')[-1]
        if not line.startswith(' CDX'):
            line = self._convert_line(line)
            yield line, ''

    def _convert_line(self, line):
        key, ts, url, length, offset, warc = line.split(' ')
        key = key.replace(')', ',)', 1)

        vals = {'o': offset, 's': length, 'w': warc, 'u': url}

        return key + ' ' + ts + ' ' + json.dumps(vals)

    def reducer_init(self):
        self.curr_lines = []
        self.curr_key = ''

        self.part_num = int(os.environ.get('mapreduce_task_partition', '0'))
        self.part_name = 'index-%05d.gz' % self.part_num

        self.gzip_temp = TemporaryFile(mode='w+b')

        self.output_dir = os.environ['mapreduce_output_fileoutputformat_outputdir']

    def reducer(self, key, values):
        if key:
            self.curr_lines.append(key)

        for x in values:
            if x:
                self.curr_lines.append(x)

        if len(self.curr_lines) == 1:
            self.curr_key = ' '.join(key.split(' ', 2)[0:2])

        if len(self.curr_lines) >= NUM_LINES:
            yield '', self._write_part()

    def reducer_final(self):
        if len(self.curr_lines) > 0:
            yield '', self._write_part()

        self._do_upload()

    def _do_upload(self):
        self.gzip_temp.flush()

        conn = boto.connect_s3()
        parts = urlparse.urlsplit(self.output_dir)

        bucket = conn.lookup(parts.netloc)

        cdxkey = bucket.new_key(parts.path + '/' + self.part_name)
        cdxkey.set_contents_from_file(self.gzip_temp, rewind=True)

        self.gzip_temp.close()

    def _write_part(self):
        z = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS + 16)

        offset = self.gzip_temp.tell()

        buff = '\n'.join(self.curr_lines)
        self.curr_lines = []

        buff = z.compress(buff)
        self.gzip_temp.write(buff)

        buff = z.flush()
        self.gzip_temp.write(buff)
        self.gzip_temp.flush()

        length = self.gzip_temp.tell() - offset

        partline = '{0}\t{1}\t{2}\t{3}'.format(self.curr_key, self.part_name, offset, length)

        return partline

if __name__ == "__main__":
    ZipNumWriterJob.run()

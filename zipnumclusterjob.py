import shutil
import sys
import os

import zlib
import urlparse
import json

from tempfile import TemporaryFile

from mrjob.job import MRJob
from mrjob.conf import combine_dicts
from mrjob.protocol import RawProtocol, RawValueProtocol


#=============================================================================
class ZipNumClusterJob(MRJob):
    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.CombineTextInputFormat'

    PARTITIONER = 'org.apache.hadoop.mapred.lib.TotalOrderPartitioner'

    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol
    INTERNAL_PROTOCOL = RawProtocol

    JOBCONF =  {'mapreduce.task.timeout': '9600000',
                'mapreduce.input.fileinputformat.split.maxsize': '50000000',
                'mapreduce.map.speculative': 'false',
                'mapreduce.reduce.speculative': 'false',
                'mapreduce.output.fileoutputformat.compress': 'false',
                'mapreduce.job.reduce.slowstart.completedmaps': '1.0',
                'mapreduce.job.jvm.numtasks': '-1'
               }

    def configure_options(self):
        """Custom command line options for indexing"""
        super(ZipNumClusterJob, self).configure_options()

        self.add_passthrough_option('--numlines', dest='numlines',
                                    type=int,
                                    default=3000,
                                    help='Number of lines per gzipped block')

        self.add_passthrough_option('--splitfile', dest='splitfile',
                                    help='Split file to use for CDX shard split')

        self.add_passthrough_option('--convert', dest='convert',
                                    action='store_true',
                                    default=False,
                                    help='Convert CDX through _convert_line() function')

        self.add_passthrough_option('--shards', dest='shards',
                                    type=int,
                                    help='Num ZipNum Shards to create, ' +
                                         '= num of entries in splits + 1' +
                                         '= num of reducers used')

    def jobconf(self):
        orig_jobconf = super(ZipNumClusterJob, self).jobconf()
        custom_jobconf = {'mapreduce.job.reduces': self.options.shards,
                          'mapreduce.totalorderpartitioner.path': self.options.splitfile}

        combined = combine_dicts(orig_jobconf, custom_jobconf)
        return combined

    def mapper_init(self):
        pass

    def mapper(self, _, line):
        line = line.split('\t')[-1]
        if not line.startswith(' CDX'):
            if self.options.convert:
                line = self._convert_line(line)
            yield line, ''

    def _convert_line(self, line):
        key, ts, url, length, offset, warc = line.split(' ')
        key = key.replace(')', ',)', 1)

        vals = {'o': offset, 's': length, 'w': warc, 'u': url}

        return key + ' ' + ts + ' ' + json.dumps(vals)

    def _get_prop(self, proplist):
        for p in proplist:
            res = os.environ.get(p)
            if res:
                return res

    def reducer_init(self):
        self.curr_lines = []
        self.curr_key = ''

        self.part_num = self._get_prop(['mapreduce_task_partition', 'mapred_task_partition'])
        assert(self.part_num)

        self.part_name = 'cdx-%05d.gz' % int(self.part_num)

        self.output_dir = self._get_prop(['mapreduce_output_fileoutputformat_outputdir',
                                          'mapred.output.dir',
                                          'mapred_work_output_dir'])

        assert(self.output_dir)
        self.gzip_temp = TemporaryFile(mode='w+b')

    def reducer(self, key, values):
        if key:
            self.curr_lines.append(key)

        for x in values:
            if x:
                self.curr_lines.append(x)

        if len(self.curr_lines) == 1:
            self.curr_key = ' '.join(key.split(' ', 2)[0:2])

        if len(self.curr_lines) >= self.options.numlines:
            yield '', self._write_part()

    def reducer_final(self):
        if len(self.curr_lines) > 0:
            yield '', self._write_part()

        self._do_upload()

    def _do_upload(self):
        self.gzip_temp.flush()
        #TODO: move to generalized put() function
        if self.output_dir.startswith('s3://') or self.output_dir.startswith('s3a://'):
            import boto
            conn = boto.connect_s3()
            parts = urlparse.urlsplit(self.output_dir)

            bucket = conn.lookup(parts.netloc)

            cdxkey = bucket.new_key(parts.path + '/' + self.part_name)
            cdxkey.set_contents_from_file(self.gzip_temp, rewind=True)
        else:
            path = os.path.join(self.output_dir, self.part_name)

            self.gzip_temp.seek(0)

            with open(path, 'w+b') as target:
                shutil.copyfileobj(self.gzip_temp, target)

        self.gzip_temp.close()

    def _write_part(self):
        z = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS + 16)

        offset = self.gzip_temp.tell()

        buff = '\n'.join(self.curr_lines) + '\n'
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
    ZipNumClusterJob.run()

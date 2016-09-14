import random
from heapq import heappush, heapreplace

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol


#=============================================================================
class SampleCDXJob(MRJob):
    """ Sample CDX key space using reservoir sampling
    MR algorithm adapted:
    http://had00b.blogspot.com/2013/07/random-subset-in-mapreduce.html
    """

    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.CombineTextInputFormat'

    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    JOBCONF =  {'mapreduce.task.timeout': '9600000',
                'mapreduce.input.fileinputformat.split.maxsize': '50000000',
                'mapreduce.map.speculative': 'false',
                'mapreduce.reduce.speculative': 'false',
                'mapreduce.job.jvm.numtasks': '-1',

                # the output should not be compressed even if the default is to compress output,
                # otherwise reading from MRJobRunner.stream_output() needs decompression on the fly
                'mapreduce.output.fileoutputformat.compress': 'false',

                'mapreduce.job.reduces': '1'
               }

    def configure_options(self):
        """Custom command line options for indexing"""
        super(SampleCDXJob, self).configure_options()

        self.add_passthrough_option('--shards', dest='shards',
                                    type=int,
                                    default=300,
                                    help='Number of shards in output ' +
                                         '(create shards-1 splits')

        self.add_passthrough_option('--scaler', dest='scaler',
                                    type=int,
                                    default=100,
                                    help='Scaler for sample size: ' +
                                         'Sample size = shards * scaler')

        self.add_passthrough_option('--splitfile', dest='splitfile',
                                    help='Split file output dest, ' +
                                         'will contain shards-1 splits')

    def mapper_init(self):
        self.N = self.options.shards * self.options.scaler
        self.H = []

    def mapper(self, _, line):
        line = line.split('\t')[-1]
        if line.startswith(' CDX'):
            return

        r = random.random()

        if len(self.H) < self.N:
            heappush(self.H, (r, line))

        elif r > self.H[0][0]:
            heapreplace(self.H, (r, line))

    def mapper_final(self):
        for (r, x) in self.H:
            # by negating the id, the reducer receives
            # the elements from highest to lowest
            yield -r, x

    def reducer_init(self):
        self.N = self.options.shards * self.options.scaler
        self.output_list = []

    def reducer(self, key, values):
        for x in values:
            if len(self.output_list) >= self.N:
                return

            self.output_list.append(x)

    def reducer_final(self):
        # sample sorted list by scaler, skip first element
        # to get a N-1 even samples from N*SCALER set
        self.output_list = sorted(self.output_list)
        self.output_list = self.output_list[0::self.options.scaler][1:]
        for x in self.output_list:
            yield '', x


if __name__ == "__main__":
    SampleCDXJob().run()

import boto
import shutil
import sys

import random
from heapq import heappush, heapreplace

from mrjob.job import MRJob
from mrjob.protocol import RawProtocol, RawValueProtocol

k = 300

class SampleCDXJob(MRJob):
    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.CombineTextInputFormat'
#    HADOOP_OUTPUT_FORMAT = 'org.apache.hadoop.mapred.SequenceFileOutputFormat'

    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    JOBCONF = {'mapreduce.job.reduces': '1',
               'mapreduce.input.fileinputformat.split.maxsize': '50000000',
               'mapreduce.map.speculative': 'false',
               'mapreduce.reduce.speculative': 'false',

#               'mapreduce.job.output.value.class': 'org.apache.hadoop.io.NullWritable'
              }

    def mapper_init(self):
        self.H = []

    def mapper(self, _, line):
        line = line.split('\t')[-1]
        if line.startswith(' CDX'):
            return

        r = random.random()

        if len(self.H) < k:
            heappush(self.H, (r, line))
        elif r > self.H[0][0]:
            heapreplace(self.H, (r, line))

    def mapper_final(self):
        for (r, x) in self.H:
            # by negating the id, the reducer receives the elements from highest to lowest
            yield -r, x

    def reducer_init(self):
        self.output_list = []

    def reducer(self, key, values):
        for x in values:
            if len(self.output_list) > k:
                return

            self.output_list.append(x)

    def reducer_final(self):
        self.output_list = sorted(self.output_list)
        for x in self.output_list:
            yield '', x


if __name__ == "__main__":
    SampleCDXJob.run()

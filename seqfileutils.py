from hadoop.io.NullWritable import NullWritable
from hadoop.io.Text import Text
from hadoop.io import SequenceFile
from argparse import ArgumentParser

import sys

def make_text_null_seq(filename, reader):
    writer = SequenceFile.createWriter(filename, Text, NullWritable)

    key = Text()
    value = NullWritable()

    for x in reader:
        key.set(x)
        writer.append(key, value)

    writer.close()

def count_file(filename):
    reader = SequenceFile.Reader(filename)

    key = Text()
    value = NullWritable()

    count = 0
    while reader.next(key, value):
        count += 1

    return count


def main():
    parser = ArgumentParser()
    parser.add_argument('seqfile')
    parser.add_argument('--copyfrom')

    parser.add_argument('--count', action='store_true')

    r = parser.parse_args()

    if r.count:
        print(count_file(r.seqfile))
    elif r.copyfrom:
        with open(r.copyfrom) as fh:
            make_text_null_seq(r.seqfile, fh)

if __name__ == "__main__":
    main()

WebArchive Url Indexing
=======================

This repo contains several scripts (MapReduce jobs) for generating indexes of web archives. Often, an archive consists of a collection
WARC and ARC files, which contains captures of web data. However, a WARC or ARC file typically do not have an index
of url to archive content. Such indexes are usually built across multiple WARC or ARC files to allow for searching the
entire archive collection. Some archived collections may span terabytes or even petabytes, and this repo provides an efficient and scalable way of creating indexes across such collections.

Tools Provided and Dependencies
-------------------------------
This repository provides three main tools, in the form of MapReduce jobs, to go from a set of WARC or ARC files to a distributed url index, in a format known as a ZipNum CDX cluster.

To accomplish this, 3 distinct jobs are provided:

1. Indexing Individual WARCs to CDX Files
2. Sampling WARCs to Create Split File
3. Generating a Distributed CDX Cluster.

This repo provides pure-Python tools for creating such indexes, either locally or via MapReduce on Hadoop or Emr.

Libraries used here are:

- [pywb web replay tools](https://github.com/ikreymer/pywb) for creating CDX indexes from WARCs and ARCs
- [MRJob](https://pythonhosted.org/mrjob/) MapReduce library for running MapReduce jobs on Hadoop, Amazon EMR or locally.
- [python-hadoop](https://github.com/matteobertozzi/Hadoop/tree/master/python-hadoop) - A python hadoop utility library for creating a hadoop SequenceFile in pure Python. (for generating split point SequenceFile)


CDX File
--------

An index for a web archive (WARC or ARC) file is often referred to as a CDX file, probably from **C**apture/**C**rawl 
in**D**e**X** **(CDX)**. A CDX file is typically a plain-text file (optionally gzip-compressed) format, with each line
representing info about a single capture in an archive. The CDX contains multiple fields, typically the url and where to
find the archived contents of that url. Unfortunately, no standardized format for CDX files exists, and there have been
many formats, usually with varying number of space-seperated fields. Here is an old reference for [CDX File](https://archive.org/web/researcher/cdx_file_format.php) (from Internet Archive). In practice, CDX files typically contain a subset of the possible fields.

While there are no required fields, in practice, the following 6 fields
are needed to identify a record: `url search key`, `url timestamp`, `original url`, `archive file`, `archive offset`, `archive length`. The search key is often the url transformed and 'canonicalized' in a way to make it easier for lexigraphic seaching.
A common transformation is to reverse subdomains `example.com` -> `com,example,)/` to allow for searching by domain, then subdomains.

The indexing job uses the flexible pywb `cdx-indexer` to create indexs of a certain format. However, the other jobs are compatible with any existing CDX format as well. Other indexing tools can be used also but require seperate integration.

Creating a CDX Index of a Single WARC/ARC
-----------------------------------------

Before continuing, it is important to point out that these tools are intended specifically for bulk indexing.

Creating an index of one or even a few WARC/ARC file is easy and does not require any of this setup. The `cdx-indexer` application which comes with the [pywb](https://github.com/ikreymer/pywb) can do this via command line:

For example:

```
cdx-indexer -s output.cdx [input1.warc.gz] [/path/to/dir/]
```
This command will create a merged, sorted index `output.cdx` from files `input1.warc.gz` and all WARC/ARC files in directory `/path/to/dir/`. If the directory contains a few WARCs, this is probably the right approach to creating an index.

The distributed indexing job uses this tool to build an index for each file in parallel (using Hadoop)




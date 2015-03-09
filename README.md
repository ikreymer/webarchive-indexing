WebArchive Bulk Indexing
========================

This repo contains several scripts for generating indexes of web archives.
The tools use the MRJob library and Hadoop to create a distributed, sorted gzip chunked
index called ZipNum from a large number of WARC/ARC files.

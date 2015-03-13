WebArchive Url Indexing
=======================

This project contains several scripts (MapReduce jobs) for generating url indexes of web archive collections, ususally containing large number of of WARC (or ARC) files. The scripts are designed to ran in Hadoop or Amazon EMR to process terabytes or even petabytes of web archive content. Additionally, thanks to flexibility of the MRJob library,
the scripts can also run on a local machine to build an index cluster.

## Initial Setup and Usage

These tools use the MRJob Python library for Hadoop/EMR, and area pure-python solution to web archive indexing.

To install [dependencies](#dependencies): `pip install -r requirements.txt`

#### Remote - EMR/Hadoop

*Note: At this time, the scripts are configured to work with EMR and have been tested with CommonCrawl data set. 
Eventually, the tools will be generalized to work on any Hadoop cluster.*

To run with MRJob library, a system-specific `mrjob.conf` needs to be configured. The file contains all the settings necessary to specify your Hadoop or EMR cluster. Refer to the [MRJob documentation for details](https://pythonhosted.org/mrjob/guides/configs-basics.html).

In addition, a bash script `index_env.sh` is used to specify all the relevant paths. 

You can simply run `cp index_env.sample.sh index_env.sh` to copy the provided sample. Please refer to the file for more details and to fill in the actual paths.

#### Local

No additional setup is necessary. See [building a local cluster](#building-a-local-cluster).

### Tools Provided

This repository provides three Hadoop MapReduce jobs to create [a shared url index](#zipnum-sharded-cdx-cluster) from an input list of WARC/ARC files. This process can be split into three jobs.

1. [Indexing Individual ARC/WARCs to CDX Files](#indexing-individual-arcwarcs-to-cdx-files)
2. [Sampling CDXs to Create Split File](#sampling-cdxs-to-create-split-file)
3. [Generating a ZipNum CDX Cluster](#generating-a-zipnum-cdx-cluster)

Each step is a mapreduce job, run with the Python MRJob library. The first step may be omitted if you already have
indexes for the WARCs.

If you have a small number of local cdx files, you also use these scripts to [build a local cluster](#building-a-local-cluster)

[Additional background info on indexing and the formats used](#additional-info).


## Indexing Individual ARC/WARCs to CDX Files ##


*Note: If you already have .cdx files for each of your WARC/ARCS, you may skip this step*

The job can be started by running:

```
runindexwarcs.sh
```

This boostraps the `indexwarcsjobs.py` script, which will start a map-reduce job to create a cdx file for each WARC/ARC file in the input.

**Input:** A manifest file of WARC/ARCs to be indexed

**Output:** A compressed cdx file (.cdx.gz) for each WARC/ARC processed.

The path of each input is kept and the extension is replaced with .cdx.gz.

Thus, for inputs:

```
/dir1/mywarc1.gz
/dir2/mywarc2.gz
```
and output directory of `/cdx/`, the following will be created:

```
/cdx/dir1/mywarc1.cdx.gz
/cdx/dir2/mywarc2.cdx.gz
```

This is a map only job, and a single mapper is created per input file by default.

The `pywb.warc.cdxindexer.write_cdx_index`, the same used by the pywb `cdx-indexer` app is used to create the index.
Refer to `cdx-indexer -h` for list of possible options.

### Sampling CDXs to Create Split File ###

This job can be started by running:

```
runsample.sh
```

The actual job, defined in  `samplecdxjob.py` determines *split points* for the cluster for an arbitrary number of splits. The final job will sort all the lines from all the CDX files into N parts (determined by number of reducers), however, in order to do so, it is necessary to determine a rough distribution of the url space.

** Input: ** A path to per-WARC CDX files (created in step 1)
** Output: ** A file containing split points to split CDX space into N shards (in hadoop SequenceFile format) to 

*Note: This step is generally only necessary the first time a cluster is created. If a subsequent cluster with similar distribution is created, it is possible to reuse an existing split file. Additionally, it will be possible to create a more accurate split file directly from an existing cluster (TODO)*

To create the split file, all the CDX files are sampled using [a reservoir sampling technique](http://had00b.blogspot.com/2013/07/random-subset-in-mapreduce.html) (This technique may need some refinement but only an *approximate* distribution is needed).

The output of this job will be a single file with N-1 split points (for N parts/shards/reducers).

The job creates a plain text file with N-1 lines.

#### Converting to SequenceFile

However, to be used with the final job, the file needs to be in a Hadoop `SequenceFile<Text, NullWritable>` format.
Fortunatelly, the `python-hadoop` library provides an easy way to convert a text file to a Hadoop SequenceFile of this format. The `dosample.py` script combines the map-reduce job with the SequenceFile conversion and then uploads the file sequencefile to final destination (currently S3 path).

### Generating a ZipNum CDX Cluster

The final job can be started by running:

```
runzipcluster.sh
```

The corresponding script, `zipnumclusterjob.py`, creates the [ZipNum Sharded CDX Cluster](#zipnum-sharded-cdx-cluster) from the individual CDX files (created in the first job) using the split file (created in the second job).

** Input: ** Per-WARC CDX files and split points file (from previous two steps)
** Output: ** Sharded compressed CDX split into N shards, with secondary index per shard

To accomplish this, the Hadoop `TotalOrderPartitioner` is used which distributes CDX records across reducers in such a way to create a total ordering along the split points. Each reducers already sorts the inputs, and the partitioner ensures the all reducers only cover their particular split of the key space.

Each reducer outputs the secondary index (one line per 3000 CDX lines), and creates a gzip file of the actual cdx lines as side-effect. Thus for each reducer, 0..N the following files are created.

* `part-N` - plain text secondary index
* `cdx-N.gz` - gzipped cdx index, concatenated chunks of X (usually 3000) CDX lines in each chunk.

After the job finishes and files are retrieved locally, running:
`cat part-* > all.idx` is all that's needed to create a binary-searchable secondary index for the ZipNum Cluster.

This index can then be used with existing tools, such as pywb and OpenWayback, which can read the index and provide a REST API for accessing the index.

## Building a local cluster

Thanks to the flexibility of the MRJob library, it is also possible to build a local ZipNum cluster, no Hadoop or EMR required! (MRJob automatically computes even split points when running locally, so the split file computation step is not necessary).

If you have a number of [CDX](#cdx-file-format) files on disk, you can use the `build_local_zipnum.py` script to directly build a cluster locally on your machine.

For example, the following will be a cluster of 25 shards. 

```
python build_local_zipnum.py /path/to/zipnum/ -s 25 -p /path/to/cdx/*.cdx.gz
```

(The `-p` flag will specify if parallel processes wil be created
for each map/reduce task, or (if absent) all tasks will be created sequentially).

After the script runs, the following files will be created:
```
/path/to/zipnum/part-000{00-24}
/path/to/zipnum/cdx-000{00-24}.gz
/path/to/zipnum/cluster.summary
/path/to/zipnum/cluster.loc
```

The `cluster.summary` and `cluster.loc` files may be used with index ZipNum cluster support in the wayback machine, including
pywb and OpenWayback.


### Dependencies

These tools depend on the following libraries/tools. If using Hadoop, they need to be installed on the cluster.
If Using EMR, the MRJob library can do this automatically when starting a new cluster, and a bootstrap script is also provided for easy installation seperate in a persistant EMR job flow.

- [pywb web replay tools](https://github.com/ikreymer/pywb) for creating CDX indexes from WARCs and ARCs
- [MRJob](https://pythonhosted.org/mrjob/) MapReduce library for running MapReduce jobs on Hadoop, Amazon EMR or locally.
- [python-hadoop](https://github.com/matteobertozzi/Hadoop/tree/master/python-hadoop) - A python hadoop utility library for creating a hadoop SequenceFile in pure Python. (for generating split point SequenceFile)


## Additional Info

This section contains a bit of background on the indexing formats used, and indexing individual files in general.

### Creating a CDX Index of a Single WARC/ARC

It is important to point out that these tools are intended specifically for bulk indexing.

Creating an index of one or even a few WARC/ARC file is also easy and does not require any of this setup. The `cdx-indexer` application which comes with the [pywb](https://github.com/ikreymer/pywb) can do this via command line:

For example:

```
cdx-indexer -s output.cdx [input1.warc.gz] [/path/to/dir/]
```
This command will create a merged, sorted index `output.cdx` from files `input1.warc.gz` and all WARC/ARC files in directory `/path/to/dir/`. If the directory contains a few WARCs, this is probably the right approach to creating an index.

The distributed indexing job uses this tool to build an index for each file in parallel (using Hadoop).

### CDX File Format

An index for a web archive (WARC or ARC) file is often referred to as a CDX file, probably from **C**apture/**C**rawl 
in**D**e**X** **(CDX)**. A CDX file is typically a sorted plain-text file (optionally gzip-compressed) format, with each line
representing info about a single capture in an archive. The CDX contains multiple fields, typically the url and where to
find the archived contents of that url. Unfortunately, no standardized format for CDX files exists, and there have been
many formats, usually with varying number of space-seperated fields. Here is an old reference for [CDX File](https://archive.org/web/researcher/cdx_file_format.php) (from Internet Archive). In practice, CDX files typically contain a subset of the possible fields.

While there are no required fields, in practice, the following 6 fields
are needed to identify a record: `url search key`, `url timestamp`, `original url`, `archive file`, `archive offset`, `archive length`. The search key is often the url transformed and 'canonicalized' in a way to make it easier for lexigraphic seaching.
A common transformation is to reverse subdomains `example.com` -> `com,example,)/` to allow for searching by domain, then subdomains.

The indexing job uses the flexible pywb `cdx-indexer` to create indexs of a certain format. However, the other jobs are compatible with any existing CDX format as well. Other indexing tools can be used also but require seperate integration.

### ZipNum Sharded CDX Cluster

A CDX file is generally accessed by doing a simple binary search through the file. This scales well to very large (multi-gigabyte) CDX files. However, for very large archives (many terabytes or petabytes), binary search across a single file has its limits.

A more scalable alternative to a single CDX file is gzip compressed chunked cluster, with a binary searchable index.
In this format, sometimes called the ZipNum or Ziplines cluster (for some X number of cdx lines zipped together), all actual
CDX lines are gzipped compressed an concatenated together. To allow for random access, the lines are gzipped in groups of X lines (often 3000, but can be anything). This allows for the full index to be spread over N number of gzipped files, but has the overhead of requiring N lines to be read for each lookup. Generally, this overhead is negligible when looking up large indexes, and non-existent when doing a range query across many CDX lines.

The goal of the last job is to create such a index, split into a number of arbitrary shards. For each shard, there is an index file and a secondary index file. At the end, the secondary index is concatenated to form the final, binary searchable index. The number of shards is variable and is equal to the number of reducers used.

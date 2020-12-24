# FASTdoopC

*FASTdoopC* is a general software framework for the efficient acquisition of FASTA/Q genomic files in a MapReduce environment.

It works in two stages. In the first stage, it can be used to automatically split an input file into blocks independently compressed using a chosen compressor and uploaded to HDFS (the distributed file system used by frameworks like Hadoop and Spark). In the second stage, it can be used during a MapReduce computation to read and decompress on-the-fly and in a distributed way a file compressed during the first stage. It runs over *Apache Hadoop* (>=3.1.1, https://hadoop.apache.org/) and *Apache Spark* (https://spark.apache.org/), and requires a *Java* compliant virtual machine (>= 1.8, https://adoptopenjdk.net).

Notice that the framework has been designed to take fully advantage of the *HDFS* distributed file system, even thanks to the adoption of the *FASTdoop* library. For this reason, it requires input data to be stored on *HDFS*.

## Usage
The software is released as a single executable jar file, **fastdoopc-1.0.0-all.jar**, that can be used both to compress input files (first stage) and to decompress them (second stage). 

### File compression
In order to compress a file, the provided **fastdoopc-1.0.0-all.jar** jar must be run from the command-line, together with the *Apache Hadoop* `hadoop jar` command, using the following syntax:

`hadoop jar fastdoopc-1.0.0-all.jar [conf-file]`

If `conf-file` is not specified, the program will look for a `uc.conf` file in the working directory. It is used to provide instructions about the codec to use for the compression.

### Configuration File

The content of the configuration file is used by our framework to know the settings to use for dealing with either compressed files or files to be compressed. This solution is the recommended one, as it does not require any programming skill. Alternatively, it is possible to define the same settings inside a Java application using the provided programming API (see the `src/main/java/Main.java` class for an example).

| Parameter        | Description           |
|:------------- |:------------- |
| `task` | The task to perform. The value can be *compression* (default) or *benchmark* (see Section Benchmarking). |
| `input` | The genomic input file. If `task=compression`, it should be a FASTA/FASTQ file and should be placed locally, instead of HDFS. |
| `output` | The output file. If `task=compression`, the name should end with the suffix ".uc" |
| `uc.codec` | The name of the codec to use in order to compress each input block. The name is case insensitive. |

### File decompression

In order to execute a MapReduce job using as input an `.uc` compressed file generated with our framework, the Hadoop developer needs to:

1. set the Hadoop configuration parameter `io.compression.codecs` with the value `universalcodec.UniversalCodec`.
2. set the Hadoop `InputFormat` class according to the original file format. If the uncompressed file has the FASTA format, the `InputFormat` class to use have to be `FASTAShortUniversalInputFormat`, instead if the file has the FASTQ format it has to be `FASTQUniversalInputFormat`.

See the `src/java/main/benchmark/BenchmarkJob.java`, developed for the benchmarking (see Section Benchmarking), as an example.

### Example 1 - Compressing a FASTQ file using SPRINGQ

In this example, the codec named *SPRINGQ* is used to compress a 16GB input file of FASTQ reads. The output file will be named as *16GB.fastq.springq.uc*. The configuration file provides the necessary parameters needed to use the SPRING compressor for FASTQ files.

```
input=16GB.fastq
output=data/16GB.fastq.spring.uc

uc.codec=springq

uc.SPRINGQ.compress.cmd=spring -c
uc.SPRINGQ.decompress.cmd=spring -d
uc.SPRINGQ.compress.ext=spring
uc.SPRINGQ.io.input.flag=-i
uc.SPRINGQ.io.output.flag=-o
```


### Example 2 - Processing a FASTQ file compressed using SPRINGQ by means of an Hadoop application

In this example, a benchmark of type `mapreduce` is performed on the input file *data/16GB.fastq.spring.uc* (for more information, see Section benchmarking). The input is read and decompressed by the [FASTdoop](https://academic.oup.com/bioinformatics/article/33/10/1575/2908860) reader and then each input split is passed to the map task, in order to perform the (partial) nucleotides counting. Then, the partial counts are computed by the reduce tasks in order to obtain the global nucleotides counting. The configuration file provides the necessary parameters needed to use the SPRING compressor for FASTQ files.

```
task=benchmark

bench=mapreduce
sequence.type=fastq

input=data/16GB.fastq.spring.uc
output=16GB.fastq.spring.uc_benchMR

uc.SPRINGQ.compress.cmd=spring -c
uc.SPRINGQ.decompress.cmd=spring -d
uc.SPRINGQ.compress.ext=spring
uc.SPRINGQ.io.input.flag=-i
uc.SPRINGQ.io.output.flag=-o
```

### Supporting a new codec through Configuration
Our framework provides the possibility to easily support new compression codecs, assuming they are avaiable through a command-line interface. Let *X* be the unique name denoting the compressor to be supported and *F* the file being processed. The configuration file should contain the following parameters:

| Parameter        | Description           |
|:------------- |:------------- |
| `uc.X.compress.cmd` | the command line to be used for compressing F using X. |
| `uc.X.decompress.cmd` | the command line to be used for decompressing F using X. |
| `uc.X.compress.ext` | the command line flag used to specify the input filename. |
| `uc.X.decompress.ext` | the command line flag used to specify the output filename. |
| `uc.X.io.input.flag` | the extension used by X for saving a compressed copy of F. |
| `uc.X.io.output.flag` | the extension used by X for saving a decompressed copy of X ( `fastq` by default). |
| `uc.X.io.reverse` | if X requires the output file name to be specified before the input file name, it is set to `True`. `False`, otherwise. |

**Note**: we are assuming that a copy of the executable codes required from the above command lines are available on all the slave nodes of the distributed system used for compressing/decompressing input files. 

## Currently supported codecs

The following codecs have been tested with our frameworks and are included, by default, in the `src\main\resources\uc.conf.sample` configuration file:

- [DSRC](https://academic.oup.com/bioinformatics/article/30/15/2213/2391485) (FASTQ)
- [FqzComp](https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0059190) (FASTQ)
- [SPRING](https://academic.oup.com/bioinformatics/article-abstract/35/15/2674/5232998?redirectedFrom=fulltext) (FASTA/Q)
- [MFCompress](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3866555/) (FASTA)

## Datasets

*FASTdoopC* has been tested by using two different datasets. The first type of dataset, referred to as *type 1* datasets, is a collection of FASTQ and FASTA files of different sizes. The FASTA files of these datasets contain a set of reads extracted uniformly at random from a collection of genomic sequences coming from the *Human genome*. The FASTQ files of these datasets contain a set of reads extracted uniformly at random from a collection of genomic sequences coming from the *Pinus Taeda genome*. The second type of dataset, referred to as *type 2* datasets, is a collection of FASTQ files corresponding to different coverages of the *H.sapiens genome*.

The datasets can be downloaded from: [datasets](https://drive.google.com/drive/folders/1leFSikUy_lwUaCIW6TEThBueRdy4-dhr?usp=sharing)

Type 1 datasets (FASTA files):
- 16GB.fasta
- 32GB.fasta
- 64GB.fasta
- 96GB.fasta

Type 1 datasets (FASTQ files):
- 16GB.fastq
- 32GB.fastq
- 64GB.fastq
- 96GB.fastq

Type 2 datasets (FASTQ files):
- hsapiens1.fastq
- hsapiens2.fastq
- hsapiens3.fastq

## Running on Amazon Web Services (AWS)

We refer the interested reader to the following link for a quick guide about the installation of *Apache Hadoop* on a free *EC2 AWS* instance: https://dzone.com/articles/how-set-multi-node-hadoop.


## Developing an Hadoop Splittable Codec for FASTA/FASTQ files
Our framework includes a library of classes useful to simplify and accelerate the development of a specialized Hadoop splittable compression codec. This solution can be preferred to the usage of the universal meta-codec when looking for the best performance possible.  More information are available in `README.md`.

<!-- More details here

Our framework includes a library of classes able to simplify and accelerate the development of a specialized Hadoop codec in the implementation of a particular splittable compressor. It is expected that this solution would allow to obtain code that is slightly faster than the one achieved using the configuration file.
 
The following is the list of Java classes:
 
- `CodecInputFormat`.  It fetches the list of compressed data blocks existing in a compressed file and sends it to all the nodes of an Hadoop cluster together with the instructionts required for their decompression. Then, it defines the input splits as containers of compressed data blocks. These operations are compressor-dependent and require the implementation of several abstract methods like `extractMetadata`, to extract the metadata from the input file, and `getDataPosition`, to point to the starting address of the first compressed data block. 
- `NativeSplittableCodec`.  Assuming the compression/decompression routines for a particular codec are available as a standard library installed on the underlying operation system, it simplifies its integration in the codec under development.
- `CodecInputStream`. It reads the compressed data blocks existing in a HDFS data block, according to the input split strategy defined by the `CodecInputFormat`. The compressed data blocks are decompressed on-the-fly by invoking the decompression function of the considered compressor and returned to the main application. Some of these operations are compressor-dependent and require the implementation of the `setParameters` abstract method. This method is used to pass to the Codec, the command-line parameters required by the compressor, e.g execution flags, in order to correctly decompress the compressed data blocks.
- `CodecDecompressor`. It decompresses the compressed data blocks given by the `CodecInputStream`. It requires the implementation of the `decompress` abstract method.
- `NativeCodecDecompressor`. It decompresses the compressed data blocks given by the `CodecInputStream`. It requires the implementation of the `decompress` method through the native interface.
 
The user that want to support a new compressor using these classes, should necessarily know the original compressor format. 

In order to natively implement a compressor, the user should use the `NativeCodecDecompressor` class and implement the JNI methods used to decompress each compressed block. Moreover, The native functions should be implemented using the `splittablecodec_NativeCodecDecompressor.hpp` header file and then, it should be generated the dynamic library `libcodec.so`. As an example, see the `src/java/main/splittablecodec/NativeCodecDecompressor.java` and the `jni/src/splittablecodec_NativeCodecDecompressor.cpp` files. 

Alternatively, the user can implement these methods directly in Java, specializing the `CodecDecompressor` class. As an example, see the `src/java/main/universalcodec/UniversalDecompressor.java` file.
-->

## Benchmarking

The framework includes two standard benchmarks that can be used to analyze the performance of currently supported compression codec as well as new ones when used to perform two reference activities. The first benchmark is about counting in a distributed way the number of nucleotides existing in a given input FASTA/Q file. The second benchmark is an extension of the first one, with all the counts returned by the first benchmark being aggregated on a single machine and returned, as output, in a new HDFS file. To run one of these benchmarks, the following parameters must be set in the configuration file.


| Parameter        | Description           |
|:------------- |:------------- |
| `bench` | The benchmark to perform. The value must be `map`, for running the first benchmark and 'mapreduce', for running the second benchmark.|
| `sequence.type` | The type of the uncompressed input data. The value can be `fasta` or `fastq`.|

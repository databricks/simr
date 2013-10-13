# Spark In MapReduce (SIMR) Documentation

## Quick Guide

This guide assumes `simr.jar` exists. If you don't have it you have to
build a Spark jumbo jar and build Simr. See below.

Place `simr` and `simr.jar` in a directory and call `simr` to get
usage information. Try this! If you get stuck, continue reading.

## Guide

Ensure the `hadoop` executable is in the PATH. If it is not, set
$HADOOP to point to the binary, or the hadoop/bin directory.

To run a Spark application, package it up as a JAR file and execute:
```shell
./simr out_dir jar_file main_class parameters
```

* `outdir` is a (absolute or relative) path in HDFS where your job's output will be stored, e.g. `/user/alig/myjob11`
* `jar_file` is a JAR file containing all your programs, e.g. `spark-examples.jar`
* `main_class` is the name of the class with a `main` method, e.g. `org.apache.spark.examples.SparkPi`
* `parameters` is a list of parameters that will be passed to your `main_class`. 
 + _Important_: the special parameter `%spark_url%` will be replaced with the Spark driver URL.

Your output will be placed in the `outdir` in HDFS, this includes output from stdout/stderr for the driver and all executors.

**Important**: to ensure that your Spark jobs terminate without
  errors, you must end your Spark programs by calling `stop()` on
  `SparkContext`. In the case of the Spark examples, this usually
  means adding `spark.stop()` at the end of `main()`.

## Example

Assuming `spark-examples.jar` exists and contains the Spark examples, the following will execute the example that computes pi in 100 partitions in parallel:
```shell
./simr pi_outdir spark-examples.jar org.apache.spark.examples.SparkPi %spark_url% 100
```

Alternatively, you can launch a Spark-shell like this:
```shell
./simr new_out_dir --shell
```

## Requirements
* Java v1.6 is required
* SIMR will ship Scala 2.9.3 and Spark 0.8 to the Hadoop cluster and execute your program with them.
* SIMR written and compiled for Hadoop v1.2.1

## Configuration

The `$HADOOP` environment variable should point at the `hadoop` binary
or its directory.

By default SIMR figures out the number of task trackers in the cluster
and launches a job that is the same size as the cluster. This can be
adjusted by supplying the command line parameter ``--size=<integer>``
to ``simr`` or setting the Hadoop configuration parameter
`simr.cluster.size`.

## Creating simr.jar and buiding SIMR

Download Spark. This needs to be the version that supports the Simr backend scheduler:
https://github.com/alig/spark

1. Run `sbt/sbt assembly` which creates a giant jumbo jar containing
all of spark in `assembly/target/scala*/`.

2. Copy this file into the Simr `lib/` directory.

3. Now build and build a jumbo jar of Simr (this will automatically
include the Spark jumbo jar), creating a jumbo-jumbo jar is done with
`sbt/sbt assembly`. The jumbo-jumbo jar can be found in
`target/scala*`.

3. Rename the jumbo-jumbo jar to `simr.jar` and put it in the same
directory as the runtime script `simr` and follow the instructions
above to execute simr.

## How it works (advanced)

SIMR launches a Hadoop MapReduce job that only contains mappers. It
ensures that a jumbo jar (simr.jar), containing Scala and Spark, gets
uploaded to the machines of the mappers. It also ensures that the job
jar you specified gets shipped to those nodes. 

Once the mappers are all running with the right dependencies in place,
SIMR uses HDFS to do leader election to elect one of the mappers as
the Spark driver. SIMR then executes your job driver, which uses a new
SIMR scheduler backend that generates and accepts driver URLs of the
form `simr://path`.  SIMR thereafter communicates the new driver URL
to all the mappers, which then start Spark executors. The executors
connect back to the driver, which executes your program. 

All output to stdout and stderr is redirected to the specified HDFS
directory. Once your job is done, the SIMR backend scheduler has
additional functionality to shut down all the executors (hence the new
required call to `stop()`).


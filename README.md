# Spark In MapReduce (SIMR) Documentation

## Quick Guide

Download the `simr` runtime script, `simr.jar`, and a version of `spark-assembly.jar` that matches
the version of Hadoop your cluster is running. If it is not provided, you will have to build it
yourself. [See below](#advanced-configuration).

* SIMR runtime script
  + [Download] (https://s3-us-west-1.amazonaws.com/simr-test/simr)
* SIMR Jar
  + [Download] (https://s3-us-west-1.amazonaws.com/simr-test/simr.jar)
* Spark Jars are provided for the following Hadoop versions:
  + [1.0.4 (HDP 1.0 - 1.2)] (https://s3-us-west-1.amazonaws.com/simr-test/spark-assembly-hadoop-1.0.4.jar)
  + [1.2.x (HDP 1.3)] (https://s3-us-west-1.amazonaws.com/simr-test/spark-assembly-hadoop-1.2.0.jar)
  + [0.20 (CDH3)] (https://s3-us-west-1.amazonaws.com/simr-test/spark-assembly-hadoop-cdh3.jar)
  + [2.0.0 (CDH4)] (https://s3-us-west-1.amazonaws.com/simr-test/spark-assembly-hadoop-cdh4.jar)

Place `simr`, `simr.jar`, and `spark-assembly*.jar` in a directory and call `simr` to get
usage information. Try running the shell! If you get stuck, continue reading.
```shell
./simr --shell
```

## Requirements

* Java v1.6 is required
* SIMR will ship Scala 2.9.3 and Spark 0.8.1 to the Hadoop cluster and execute your program with them.
* Spark jars are provided for Hadoop 1.0.4 (HDP 1.0 - 1.2), 1.2.x (HDP 1.3), 0.20 (CDH3), 2.0.0 (CDH4)

## Guide

Ensure the `hadoop` executable is in the PATH. If it is not, set
$HADOOP to point to the binary, or the hadoop/bin directory.

To run a Spark application, package it up as a JAR file and execute:
```shell
./simr jar_file main_class parameters [--outdir=<hdfs_out_dir>] [--slots=N] [--unique]
```

* `jar_file` is a JAR file containing all your programs, e.g. `spark-examples.jar`
* `main_class` is the name of the class with a `main` method, e.g. `org.apache.spark.examples.SparkPi`
* `parameters` is a list of parameters that will be passed to your `main_class`.
 + _Important_: the special parameter `%spark_url%` will be replaced with the Spark driver URL.
* `outdir` is an optional parameter which sets the path (absolute or relative) in HDFS where your
  job's output will be stored, e.g. `/user/alig/myjob11`.
  + If this parameter is not set, a directory will be created using the current time stamp in the
    form of `yyyy-MM-dd_kk_mm_ss`, e.g.  `2013-12-01_11_12_13`
* `slots` is an optional parameter that specifies the number of Map slots SIMR should utilize.  By
  default, SIMR sets the value to the number of nodes in the cluster.
  + This value must be at least 2, otherwise no executors will be present and the task will never
    complete.
* `unique` is an optional parameter which ensures that each node in the cluster will run at most 1
  SIMR executor.

Your output will be placed in the `outdir` in HDFS, this includes output from stdout/stderr for the driver and all executors.

**Important**: to ensure that your Spark jobs terminate without
  errors, you must end your Spark programs by calling `stop()` on
  `SparkContext`. In the case of the Spark examples, this usually
  means adding `spark.stop()` at the end of `main()`.

## Example

Assuming `spark-examples.jar` exists and contains the Spark examples, the following will execute the example that computes pi in 100 partitions in parallel:
```shell
./simr spark-examples.jar org.apache.spark.examples.SparkPi %spark_url% 100
```

Alternatively, you can launch a Spark-shell like this:
```shell
./simr --shell
```

## Configuration

The `$HADOOP` environment variable should point at the `hadoop` binary
or its directory.

By default SIMR figures out the number of task trackers in the cluster
and launches a job that is the same size as the cluster. This can be
adjusted by supplying the command line parameter ``--slots=<integer>``
to ``simr`` or setting the Hadoop configuration parameter
`simr.cluster.slots`.

## Advanced Configuration

The following sections are targeted at users who aim to run SIMR on versions of Hadoop for which
Spark jars have not been provided. **It is only necessary to build the appropriate version of
spark-assembly*.jar and place it in the same directory as `simr` and `simr.jar`.** Instructions to
build simr.jar are included for completeness.

## Building Spark

In order to build SIMR, we must first compile a version of Spark that targets the version of Hadoop
that SIMR will be run on.

1. Download Spark v0.8.1 or greater.

2. Unpack and enter the Spark directory.

3. Modify `project/SparkBuild.scala`
  + Change the value of `DEFAULT_HADOOP_VERSION` to match the version of Hadoop you are targeting, e.g.
  `val DEFAULT_HADOOP_VERSION = "1.2.0"`

4. Run `sbt/sbt assembly` which creates a giant jumbo jar containing all of Spark in
   `assembly/target/scala*/spark-assembly-<spark-version>-SNAPSHOT-<hadoop-version>.jar`.

## Building SIMR and simr.jar (Optional)

1. Checkout the SIMR repository from https://github.com/databricks/simr.git

2. Copy the Spark jumbo jar into the SIMR `lib/` directory.
  + **Important**: Ensure the Spark jumbo jar is named `spark-assembly.jar` when placed in the `lib/` directory,
    otherwise it will be included in the SIMR jumbo jar.

3. Run `sbt/sbt assembly` in the root of the SIMR directory. This will build the SIMR jumbo jar
   which will be output as `target/scala*/simr.jar`.

4. Copy `target/scala*/simr.jar` to the same directory as the runtime script `simr` and follow the
   instructions above to execute SIMR.

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


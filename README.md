# Spark In MapReduce (SIMR) Documentation

## Quick Guide

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

## Requirements
* Java v1.6 is required
* SIMR will ship Scala 2.9.3 and Spark 0.8 to the Hadoop cluster and execute your program with them.

## How it works

SIMR launches a Hadoop MapReduce job that only contains mappers. It
ensures that a jumbo jar (simr.jar), containing Scala and Spark, gets
uploaded to the machines of the mappers. It also ensures that the job
jar you specified gets shipped to those nodes. The mappers use HDFS to
do leader election to elect one of the mappers as the Spark
driver. SIMR then executes your driver and communicates the driver URL
(through %spark_url%) to a bunch of executors on the rest of the
machines (there is a SIMR specified backend scheduler in Spark). The
executors connect back to the driver, which executes your program. All
output to stdout and stderr is redirected to the specified HDFS
directory. Once your job is done, the SIMR backend scheduler shuts
down all the executors (hence the required call to `stop()`).


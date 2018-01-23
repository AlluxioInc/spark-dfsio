# spark-dfsio

DFSIO starts several Spark tasks, and each Spark task either writes out a large file, or reads in a large file. The aggregate throughput is computed and reported.

## Prerequisites

A cluster with Spark 2.1+. To test Alluxio performance, Alluxio needs to be installed with workers colocated with Spark executors.

## Building the benchmark

The benchmark can be built with maven. Before building the benchmark, please check `pom.xml` and update Spark and Hadoop version strings to be consistent with your cluster installation. 
Run `mvn install` on the root folder of the repository to start the build. You can find the jar file at `target/benchmarks-1.0.0-SNAPSHOT-jar-with-dependencies.jar`.

## Running DFSIO

The following command will run the benchmark.

``` bash
$ spark-submit \
--class alluxio.benchmarks.TestDFSIO \
--conf "spark.scheduler.minRegisteredResourcesRatio=1" \
--conf "spark.scheduler.maxRegisteredResourcesWaitingTime=60s" \
--conf "spark.executor.extraJavaOptions=-Dalluxio.user.block.size.bytes.default=128MB -Dalluxio.user.file.readtype.default=NO_CACHE -Dalluxio.user.file.writetype.default=MUST_CACHE" \
benchmarks-1.0.0-SNAPSHOT-jar-with-dependencies.jar -p 8 -s 6144 -o w -b alluxio://<MASTER_HOSTNAME>:19998/testdfsio/
```

This will launch the `alluxio.benchmarks.TestDFSIO` job on the Spark cluster.

This test takes the following parameters:
* `-p <number of tasks>`: This controls the number of tasks to use for the test
* `-s <size of write in MB>`: This is the size of the file to write for each task, in MB
* `-o <sequence of operations>`: This is a string of 'w' or 'r' which determine the test sequence. For example, `-o wwwwwrrrrr` means the test will do the write portion of the test 5 times, then do the read portion of the test 5 times. The output of the test will be 10 sets of results. 
Note the read operation requires data to be written by write operation with same file size and number of partitions/tasks before. You can run write operation either in the same job before read or in a separate job before the read job.

* `-b <base directory>`: This is the base directory to write files or read files

Note: To get accurate throughput, run the benchmark with number of tasks no more than the total number of tasks that can be run on Spark cluster concurrently. Usually this number is determined through `--num-executors` and `--executor-cores` options in the `spark-submit` command line. E.g. if the benchmark is run with `--num-executors 8 --executor-cores 4` then the maximum tasks can be run concurrently in the cluster is 32(assuming there are enough cpu cores in the cluster to start all executors).

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.simr;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SimrJob {
    private static final String SPARKJAR = "spark.jar"; // Spark assembly jar
    private static final String SIMRTMPDIR = "simr-meta"; // Main HDFS directory used for SimrJob
    private static final String SIMRVER = "0.6";
    CmdLine cmd;

    public SimrJob(String[] args) {
        cmd = new CmdLine(args);
        cmd.parse();
    }

    public static class MyMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            final Context ctx = context;
            Timer timer = new Timer(true);
            timer.scheduleAtFixedRate(new TimerTask() { public void run() { ctx.progress(); } }, 0, 8*60*1000);

            Simr simr = new Simr(context);
            simr.run();
        }
    }

    static class ClusterSizeJob extends Configured implements Tool {

        int clusterSize = -1;

        synchronized void setClusterSize(int size) {
            clusterSize = size;
        }

        synchronized int getClusterSize() {
            return clusterSize;
        }

        public int run(String[] args) throws Exception {
            JobConf job = new JobConf(getConf());
            JobClient client = new JobClient(job);
            ClusterStatus cluster = client.getClusterStatus();
            setClusterSize(cluster.getTaskTrackers());
            return 0;
        }

    }

    public void checkParams() {
        String[] args = cmd.getArgs();

        if ( (!cmd.containsCommand("shell") && args.length < 4) || (cmd.containsCommand("shell") && args.length < 1) ) {
            System.err.println("Usage: SimrJob --shell <out_dir> [<optional>]");
            System.err.println("Usage: SimrJob <out_dir> <your_jar_file> <main_class> <your_params> [<optional>]");
            System.err.println("\n  --shell launches a Spark shell in the cluster with a remote interface on this node");
            System.err.println("  <your_params> will be passed to your <main_class>");
            System.err.println("  The string %spark_url% will be replaced with the SPARK master URL\n");
            System.err.println("<optional> SIMR has several optional parameters, these include:\n");
            System.err.println("  --slots=<num>      Requests num slots on the cluster. Needs to be at least 2.");
            System.err.println("                     The default is the number of task trackers in the cluster. ");
            System.err.println("  --interface=<num>  Spark driver will bind to a particular network interface number.");
            System.err.println("                     The default is the first interface, e.g. --interface=0.");
            System.exit(1);
        }

        if (cmd.containsCommand("shell"))
            return;

        String jar_file = args[1];
        String main_class = args[2];

        File file = new File(jar_file);
        if (!file.exists()) {
            System.err.println("SimrJob ERROR: Couldn't find specified jar file (" + jar_file + ")");
            System.exit(1);
        }

        Class myClass = null;
        try {
            URL jarUrl = new File(jar_file).toURI().toURL();
            URLClassLoader mainCL = new URLClassLoader(new URL[]{jarUrl});
            myClass = Class.forName(main_class, true, mainCL);
        } catch(ClassNotFoundException ex) {
            System.err.println("SimrJob ERROR: Couldn't find specified class (" + main_class + ")");
            System.exit(2);
        } catch(Exception ex) {
            ex.printStackTrace();
            System.exit(2);
        }

        try {
            myClass.getDeclaredMethod("main", new Class[]{String[].class});
        } catch (Exception ex) {
            System.err.println("SimrJob ERROR: Specified class doesn't have an accessible static main method");
            System.exit(2);
        }

    }

    public void updateConfig(Configuration conf) {

        conf.set("mapreduce.user.classpath.first", "true"); // important: ensure hadoop jars come last
        conf.set("mapred.map.tasks.speculative.execution", "false");
        conf.setInt("mapreduce.map.maxattempts", 1); // don't rerun if it crashes, needed in case Spark System.exit()'s
        conf.setInt("mapred.map.max.attempts", 1); // don't rerun if it crashes, needed in case Spark System.exit()'s

        String[] args = cmd.getArgs();

        if (cmd.containsCommand("memory")) {
            conf.set("mapred.child.java.opts", "-Xmx" + cmd.getCmd("memory").val + "m");
        }

        String out_dir = args[0];

        Path tmpPath = new Path(out_dir, SIMRTMPDIR);
        conf.set("simr_tmp_dir", tmpPath.toString());
        conf.set("simr_out_dir", out_dir);

        int clusterSize = -1;

        if (cmd.getIntValue("slots") != null) {
            clusterSize = cmd.getIntValue("slots");
        } else if (conf.get("simr.cluster.slots") != null) {
            clusterSize = Integer.parseInt(conf.get("simr.cluster.slots"));
        } else {
            try {
                ClusterSizeJob clusterSizeJob = new ClusterSizeJob();
                ToolRunner.run(new Configuration(), clusterSizeJob, args);
                clusterSize = Math.max(clusterSizeJob.getClusterSize(), 2);
            } catch (Exception ex) {
                System.err.println("Couldn't find out cluster size.\n\n");
                ex.printStackTrace();
            }
        }

        conf.set("simr_cluster_slots", Integer.toString(clusterSize));
        if (clusterSize < 2)
            System.err.println("WARNING! SIMR needs to run on at least 2 slots to work (1 driver + 1 executor)\n" +
                    "You can manually set the slot size with --slot=<integer>");

        int iface = 0;
        if (cmd.getIntValue("interface") != null) {
            iface = cmd.getIntValue("interface");
        }
        conf.setInt("simr_interface", iface);

        if (cmd.containsCommand("unique")) {
            conf.set("simr_unique", "true");
        } else {
            conf.set("simr_unique", "false");
        }

        if (cmd.containsCommand("shell")) {
            conf.set("simr_main_class", "org.apache.spark.simr.SimrRepl");
            conf.set("simr_rest_args", "%spark_url%");
        } else {
            String jar_file = args[1];
            String main_class = args[2];

            String rest_args = ""; // all of the rest of the args joined together
            for (int x = 3; x < args.length; x++) {
                rest_args += args[x];
                if (x < args.length-1)
                    rest_args += " ";
            }

            conf.set("simr_jar_file", jar_file);
            conf.set("simr_main_class", main_class);
            conf.set("simr_rest_args", rest_args);
        }
    }

    public Job setupJob(Configuration conf) throws Exception {

        String[] jarArgs = new String[]{};
        if (!cmd.containsCommand("shell"))
            jarArgs = new String[]{"-libjars", conf.get("simr_jar_file")};

        String[] otherArgs = new GenericOptionsParser(conf, jarArgs).getRemainingArgs();

        Job job = new Job(conf, "Simr v" + SIMRVER);

        job.setNumReduceTasks(0);  // no reducers needed
        job.setJarByClass(SimrJob.class);
        job.setMapperClass(MyMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SimrInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(conf.get("simr_out_dir")));
        return job;
    }

    public boolean run() throws Exception {
        checkParams();
        Configuration conf = new Configuration();
        updateConfig(conf);
        String[] program_args;

        System.err.println("         _               \n" +
                "   _____(_)___ ___  _____\n" +
                "  / ___/ / __ `__ \\/ ___/\n" +
                " (__  ) / / / / / / /    \n" +
                "/____/_/_/ /_/ /_/_/      version " + SIMRVER + "\n");

        System.err.println("Requesting a SIMR cluster with " + conf.get("simr_cluster_slots") + " slots");

        Job job = setupJob(conf);

        boolean retBool = true;

        job.submit();

        if (cmd.containsCommand("shell")) {
            program_args = new String[]{conf.get("simr_tmp_dir") + "/" + Simr.RELAYURL};
        } else {
            program_args = new String[]{conf.get("simr_tmp_dir") + "/" + Simr.RELAYURL,
                "--readonly"};
        }

        org.apache.spark.simr.RelayClient.main(program_args);

        retBool = job.waitForCompletion(false);

        FileSystem fs = FileSystem.get(conf);
        for (FileStatus fstat : fs.listStatus(new Path(conf.get("simr_out_dir")))) {  // delete output files
            if (fstat.getPath().getName().startsWith("part-m-")) {
                fs.delete(fstat.getPath(), false);
            }
        }

        fs.delete(new Path(conf.get("simr_tmp_dir")), true); // delete tmp dir

        System.err.println("Output logs can be found in hdfs://" + new Path(conf.get("simr_out_dir")));
        return retBool;
    }

    public static void main(String[] args) throws Exception {
        SimrJob simrJob = new SimrJob(args);
        System.exit(simrJob.run() ? 0 : 1);
    }
}

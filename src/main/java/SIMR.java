/**
 * Created with IntelliJ IDEA.
 * User: alig
 * Date: 8/23/13
 * Time: 2:14 PM
 * To change this template use File | Settings | File Templates.
 */

import java.io.IOException;
import java.util.*;
import java.net.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import scala.Tuple2;
import spark.api.java.*;
import spark.api.java.function.Function;

public class SIMR {

	static class RandomInputFormat extends InputFormat<Text, Text> {
		/**
		 * Generate the requested number of file splits, with the filename
		 * set to the filename of the output file.
		 */
		public List<InputSplit> getSplits(JobContext context) throws IOException {
			Configuration conf = context.getConfiguration();
			int clusterSize = Integer.parseInt(conf.get("simr-cluster-size"));
			InputSplit[] result = new InputSplit[clusterSize];

			for(int i=0; i < result.length; ++i) {
				result[i] = new FileSplit(new Path("dummy-split-" + i), 0, 1,
						(String[])null);
			}
			return Arrays.asList(result);
		}

		/**
		 * Return a single record (filename, "") where the filename is taken from
		 * the file split.
		 */
		static class RandomRecordReader extends RecordReader<Text, Text> {
			Path name;
			boolean first = true;

			public void initialize(InputSplit split, TaskAttemptContext context)  {
				name = ((FileSplit) split).getPath();
			}

			public boolean nextKeyValue() {
				if (first) {
					first = false;
					return true;
				}
				return false;
			}

			public Text getCurrentKey() {
				return new Text(name.getName());
			}

			public Text getCurrentValue() {
				return new Text("");
			}

			public void close() { }

			public float getProgress() { return 0.0f; }
		}

		public RecordReader<Text, Text> createRecordReader(InputSplit split,
														   TaskAttemptContext context) {
			return new RandomRecordReader();
		}
	}

	public static String getLocalIP() {
		String ip;
		try {
			Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			while (interfaces.hasMoreElements()) {
				NetworkInterface iface = interfaces.nextElement();

				if (iface.isLoopback() || !iface.isUp())
					continue;

				Enumeration<InetAddress> addresses = iface.getInetAddresses();
				while(addresses.hasMoreElements()) {
					InetAddress addr = addresses.nextElement();
					ip = addr.getHostAddress();
					return ip;
				}
			}
		} catch (SocketException e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	public static int startMasterAndGetPort(String masterIP) {
		Tuple2 tuple2 = spark.deploy.master.Master.startSystemAndActor(masterIP, 0, 8080);
		int port = ((Integer) tuple2._2());
		return port;
	}

	public static void startWorker(String masterIP, int masterPort) {
		spark.deploy.worker.Worker.main(new String[]{"spark://" + masterIP + ":" + masterPort});
		try {
			Thread.sleep(180000);
		} catch(Exception ex) {}
	}

	public static class MyMapper
			extends Mapper<Object, Text, Text, Text>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			Configuration conf = new Configuration();
			Configuration conf2 = context.getConfiguration();
			String tmpStr = conf2.get("simr-tmpdir");
			FileSystem fs = FileSystem.get(conf);


			try {
				fs.mkdirs(new Path(tmpStr));
			} catch (Exception ex) {}

			String myIP = getLocalIP();
			FSDataOutputStream outf = fs.create(new Path(tmpStr + "/" + myIP), true);
			outf.close();

			long firstMapperTime = Long.MAX_VALUE;
			String firstMapperIP = "";
			for (FileStatus fstat : fs.listStatus(new Path(tmpStr + "/"))) {
				long modTime = fstat.getModificationTime();
				if (modTime < firstMapperTime) {
					firstMapperTime = modTime;
					firstMapperIP = fstat.getPath().getName();
				}

			}

			context.write(new Text(firstMapperIP),new Text("SPARK MASTER"));

			if (myIP.equals(firstMapperIP)) {
				int mport = startMasterAndGetPort(firstMapperIP);
				try {
					Thread.sleep(3000);
				} catch(Exception ex) {}
				context.write(new Text(myIP),new Text("Starting Spark Master on port " + mport));

				FSDataOutputStream portfile = fs.create(new Path(tmpStr + "/masterport"), true);
				portfile.writeInt(mport);
				portfile.close();
				try {
					Thread.sleep(480000);
				} catch(Exception ex) {}
			} else {
				boolean gotMasterPort = false;
				int MAXTRIES = 3;
				int tries = 0;
				int mport = -1;
				while (!gotMasterPort && tries++ < MAXTRIES) {
					FileStatus[] lsArr = fs.listStatus(new Path(tmpStr + "/masterport"));
					if (lsArr.length != 0 && lsArr[0].getLen() > 0) {
						gotMasterPort = true;
						FSDataInputStream inPortFile =  fs.open(new Path(tmpStr + "/masterport"));
						mport = inPortFile.readInt();
						inPortFile.close();
					} else  {
						context.write(new Text(myIP),new Text("Sleeping...try " + tries));
						try {
							Thread.sleep(4000);
						} catch(Exception ex) {}
					}
				}
				if (gotMasterPort) {
					context.write(new Text(myIP),new Text("Starting Spark Worker on port " + mport));
					startWorker(firstMapperIP, mport);
				}
			}

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

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] args2 = new String[]{"-libjars","/root/perfecto.jar"};

		List <String> argList = new LinkedList<String>();
		for (String arg : args2) { argList.add(arg); }
		for (String arg : args) { argList.add(arg); }

		String[] allArgs = argList.toArray(new String[]{});

		String[] otherArgs = new GenericOptionsParser(conf, allArgs).getRemainingArgs();
		if (otherArgs.length < 1) {
			System.err.println("Usage: SIMR <out>");
			System.exit(2);
		}

		ClusterSizeJob clusterSizeJob = new ClusterSizeJob();
		int res = ToolRunner.run(new Configuration(), clusterSizeJob, args);

		int clusterSize = clusterSizeJob.getClusterSize();

		System.err.println("Cluster size: " + clusterSize);

		conf.set("simr-cluster-size", Integer.toString(clusterSize));

		String outDir = otherArgs[0];

		Path tmpPath = new Path(outDir, "simr-meta");

		conf.set("simr-tmpdir", tmpPath.toString());

		conf.set("mapreduce.user.classpath.first", "true");

		Job job = new Job(conf, "SIMR4");

		job.setNumReduceTasks(0);
		job.setJarByClass(SIMR.class);
		job.setMapperClass(MyMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(RandomInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(outDir));

		URLClassLoader urlCL = (URLClassLoader)conf.getClassLoader();


		for (URL url : ((URLClassLoader)conf.getClassLoader()).getURLs()) {
			System.out.println("cl ==> " + url.getFile() );
		}
		String files = conf.get("tmpfiles");
		String libjars = conf.get("tmpjars");
		String archives = conf.get("tmparchives");
		System.out.println("files = " + files);
		System.out.println("libjars = " + libjars);
		System.out.println("archives = " + archives);


		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

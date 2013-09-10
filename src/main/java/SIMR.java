/**
 * Created with IntelliJ IDEA.
 * User: alig
 * Date: 8/23/13
 * Time: 2:14 PM
 * To change this template use File | Settings | File Templates.
 */

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
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

import scala.Tuple3;


public class SIMR {

	private static final String SIMRTMPDIR = "simr-meta";
	private static final String ELECTIONDIR = "election";
	private static final String DRIVERURL = "driverurl";

	static class RandomInputFormat extends InputFormat<Text, Text> {
		/**
		 * Generate the requested number of file splits, with the filename
		 * set to the filename of the output file.
		 */
		public List<InputSplit> getSplits(JobContext context) throws IOException {
			Configuration conf = context.getConfiguration();
			int clusterSize = Integer.parseInt(conf.get("simr_cluster_size"));
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
		Tuple3 tuple3 = org.apache.spark.deploy.master.Master.startSystemAndActor(masterIP, 0, 8080);
		int port = ((Integer) tuple3._2());
		return port;
	}

	public static void startWorker(String masterUrl, int uniqueId) {
		String[] exList = new String[]{masterUrl, Integer.toString(uniqueId), getLocalIP(), "1"};
		org.apache.spark.executor.StandaloneExecutorBackend.main(exList);
//		org.apache.spark.deploy.worker.Worker.main(new String[]{"spark://" + masterIP + ":" + masterPort});
		try {
			Thread.sleep(180000);
		} catch(Exception ex) {}
	}

	public static class MyMapper
			extends Mapper<Object, Text, Text, Text>{

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);

			String simrDirName = conf.get("simr_tmp_dir");
			String electionDirName = simrDirName + "/" + ELECTIONDIR;

			try {
				fs.mkdirs(new Path(electionDirName));
			} catch (Exception ex) {}

			String myIP = getLocalIP();
			Path myIpFile = new Path(electionDirName + "/" + myIP);
			FSDataOutputStream outf = fs.create(myIpFile, true);
			outf.close();



			int myUniqueId = context.getTaskAttemptID().getTaskID().getId();

			long firstMapperTime = Long.MAX_VALUE;
			String firstMapperIP = "";
			for (FileStatus fstat : fs.listStatus(new Path(electionDirName + "/"))) {
				long modTime = fstat.getModificationTime();
				if (modTime < firstMapperTime) {
					firstMapperTime = modTime;
					firstMapperIP = fstat.getPath().getName();
				}

			}

			context.write(new Text(firstMapperIP),new Text("SPARK MASTER"));

			if (myIP.equals(firstMapperIP)) {

				String master_url = "simr://" + simrDirName + "/" + DRIVERURL;
				String main_class = conf.get("simr_main_class");
				String rest_args = conf.get("simr_rest_args");

				System.err.println("rest_args from conf: " + rest_args);

				String[] program_args = rest_args.replaceAll("\\%master\\%", master_url).split(" ");
				int tmpx = 0;
				for (String s : program_args) {
					System.err.println(tmpx + " : " + s);
					tmpx++;
				}

				try {
					URLClassLoader mainCL = new URLClassLoader(new URL[]{}, this.getClass().getClassLoader());
					Class myClass = Class.forName(main_class, true, mainCL);
					Method method = myClass.getDeclaredMethod("main", new Class[]{String[].class});
					//      Object instance = myClass.newInstance();
					Object result = method.invoke(null, new Object[]{program_args});
				} catch (Exception ex) { System.out.println(ex); }



				try {
					Thread.sleep(480000);
				} catch(Exception ex) {}
			} else {
				boolean gotDriverUrl = false;
				int MAXTRIES = 10;
				int tries = 0;
				String mUrl = "";
				Path driverFile = new Path(simrDirName + "/" + DRIVERURL);
				System.err.println("Looking for file: " + driverFile);
				while (!gotDriverUrl && tries++ < MAXTRIES) {
					FileStatus [] lsArr = fs.listStatus(driverFile);
					if (lsArr.length != 0 && lsArr[0].getLen() > 0) {
						gotDriverUrl = true;
						FSDataInputStream inPortFile =  fs.open(driverFile);
						mUrl = inPortFile.readUTF();
						inPortFile.close();
					} else  {
						context.write(new Text(myIP),new Text("Sleeping...try " + tries));
						try {
							Thread.sleep(4000);
						} catch(Exception ex) {}
					}
				}
				if (gotDriverUrl) {
					context.write(new Text(myIP),new Text("Starting Spark Worker on port " + mUrl));
					startWorker(mUrl, myUniqueId);
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

	public static void checkParams(String[] args) {
		String jar_file = args[1];
		String main_class = args[2];

		if (args.length < 4) {
			System.err.println("Usage: SIMR <out_dir> <your_jar_file> <main_class> <your_params>");
			System.err.println("\n<your_params> will be passed to your <main_class>");
			System.err.println("The string %master% will be replaced with the SPARK master URL");
			System.exit(1);
		}

		File file = new File(jar_file);
		if (!file.exists()) {
			System.err.println("SIMR ERROR: Coudln't find specified jar file (" + jar_file + ")");
			System.exit(1);
		}

		Class myClass = null;
		try {
			URL jarUrl = new File(jar_file).toURI().toURL();
			URLClassLoader mainCL = new URLClassLoader(new URL[]{jarUrl});
			myClass = Class.forName(main_class, true, mainCL);
		} catch(ClassNotFoundException ex) {
			System.err.println("SIMR ERROR: Couldn't find specified class (" + main_class + ")");
			System.exit(2);
		} catch(Exception ex) {
			ex.printStackTrace();
			System.exit(2);
		}

		try {
			myClass.getDeclaredMethod("main", new Class[]{String[].class});
		} catch (Exception ex) {
			System.err.println("SIMR ERROR: Specified class doesn't have an accessible static main method");
			System.exit(2);
		}

	}

	public static void updateConfig(Configuration conf, String[] args) {
		String out_dir = args[0];
		String jar_file = args[1];
		String main_class = args[2];

		String rest_args = ""; // all of the rest of the args joined together
		for (int x = 3; x < args.length; x++) {
			rest_args += args[x];
			if (x < args.length-1)
				rest_args += " ";
		}

		Path tmpPath = new Path(out_dir, SIMRTMPDIR);
		conf.set("simr_tmp_dir", tmpPath.toString());
		conf.set("simr_out_dir", out_dir);
		conf.set("simr_jar_file", jar_file);
		conf.set("simr_main_class", main_class);
		conf.set("simr_rest_args", rest_args);

		int clusterSize = -1;

		if (conf.get("simr_cluster_size") != null) {
			clusterSize = Integer.parseInt(conf.get("simr_cluster_size"));
		} else {
			try {
				ClusterSizeJob clusterSizeJob = new ClusterSizeJob();
				ToolRunner.run(new Configuration(), clusterSizeJob, args);
				clusterSize = clusterSizeJob.getClusterSize();
			} catch (Exception ex) {
				System.err.println("Coudln't find out cluster size.\n\n");
				ex.printStackTrace();
			}
		}

		System.err.println("Setting up a cluster of size (simr_cluster_size): " + clusterSize);

		conf.set("simr_cluster_size", Integer.toString(clusterSize));

		conf.set("mapreduce.user.classpath.first", "true"); // important: ensure hadoop jars come last
		conf.set("mapred.map.tasks.speculative.execution", "false");
	}

	public static Job setupJob(Configuration conf) throws Exception {
		String[] jarArgs = new String[]{"-libjars", conf.get("simr_jar_file")}; // hadoop ships jars
		String[] otherArgs = new GenericOptionsParser(conf, jarArgs).getRemainingArgs();


		Job job = new Job(conf, "Spark In MapReduce (Simr)");

		job.setNumReduceTasks(0);  // no reducers needed
		job.setJarByClass(SIMR.class);
		job.setMapperClass(MyMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(RandomInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(conf.get("simr_out_dir")));
		return job;
	}

	public static void main(String[] args) throws Exception {
		checkParams(args);

		Configuration conf = new Configuration();

		updateConfig(conf, args);

		Job job = setupJob(conf);

		System.exit(job.waitForCompletion(true) ? 0 : 1); // block until job finishes
	}

}

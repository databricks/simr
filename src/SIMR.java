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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SIMR {

	static class RandomInputFormat extends InputFormat<Text, Text> {
		/**
		 * Generate the requested number of file splits, with the filename
		 * set to the filename of the output file.
		 */
		public List<InputSplit> getSplits(JobContext context) throws IOException {
			Configuration cf = context.getConfiguration();
			Map<String,String> m = new TreeMap<String,String>();
			for (Map.Entry<String, String> entry : cf) {
				m.put(entry.getKey(), entry.getValue());
			}
			for(Map.Entry<String,String> entry : m.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();

				try {
				if (Integer.parseInt(value) == 2)
					System.out.println("**");
				System.out.println(key + " => " + value);
				if (Integer.parseInt(value) == 2)
					System.out.println("**");
				} catch (Exception ex) {}
			}
//			int clusterSize = new JobClient().getClusterStatus().getTaskTrackers();
			int clusterSize = 1;
			InputSplit[] result = new InputSplit[clusterSize];
//			Path outDir = org.apache.hadoop.mapred.FileOutputFormat.getOutputPath(job);
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

			FSDataOutputStream outf = fs.create(new Path(tmpStr + "/" + getLocalIP()), true);
			outf.close();


//			String p = context.getConfiguration().get("passing");
//			context.write(new Text(p),new Text(""));

		}
	}

	static class DummyConf extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			if (args.length == 0) {
				System.out.println("Usage: writer <out-dir>");
				ToolRunner.printGenericCommandUsage(System.out);
				return -1;
			}

			Path outDir = new Path(args[0]);
			JobConf job = new JobConf(getConf());

			job.setJarByClass(DummyConf.class);
			job.setJobName("random-writer");
			org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(job, outDir);

			job.setOutputKeyClass(BytesWritable.class);
			job.setOutputValueClass(BytesWritable.class);

//			job.setInputFormat(DummyConf.class);
//			job.setMapperClass(Map.class);
			job.setReducerClass(IdentityReducer.class);
			job.setOutputFormat(SequenceFileOutputFormat.class);

			JobClient client = new JobClient(job);
			ClusterStatus cluster = client.getClusterStatus();

			System.out.println("System size = " + cluster.getTaskTrackers());

			return 0;
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: SIMR <out>");
			System.exit(2);
		}

		int res = ToolRunner.run(new Configuration(), new DummyConf(), args);

		String outDir = otherArgs[0];

		Path tmpPath = new Path(outDir, "simr-meta");

		conf.set("simr-tmpdir", tmpPath.toString());
 		Job job = new Job(conf, "SIMR3");

		job.setNumReduceTasks(0);
		job.setJarByClass(SIMR.class);
		job.setMapperClass(MyMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(RandomInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(outDir));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SIMR extends Configured implements Tool {

	static class RandomInputFormat implements InputFormat<Text, Text> {

		/**
		 * Generate the requested number of file splits, with the filename
		 * set to the filename of the output file.
		 */
		public InputSplit[] getSplits(JobConf job,
									  int numSplits) throws IOException {
//			int numSplits = new JobClient().getClusterStatus().getTaskTrackers();
			InputSplit[] result = new InputSplit[numSplits];
			Path outDir = org.apache.hadoop.mapred.FileOutputFormat.getOutputPath(job);
			for(int i=0; i < result.length; ++i) {
				result[i] = new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1,
						(String[])null);
			}
			return result;
		}

		/**
		 * Return a single record (filename, "") where the filename is taken from
		 * the file split.
		 */
		static class RandomRecordReader implements RecordReader<Text, Text> {
			Path name;
			public RandomRecordReader(Path p) {
				name = p;
			}
			public boolean next(Text key, Text value) {
				if (name != null) {
					key.set(name.getName());
					name = null;
					return true;
				}
				return false;
			}
			public Text createKey() {
				return new Text();
			}
			public Text createValue() {
				return new Text();
			}
			public long getPos() {
				return 0;
			}
			public void close() {}
			public float getProgress() {
				return 0.0f;
			}
		}

		public RecordReader<Text, Text> getRecordReader(InputSplit split,
														JobConf job,
														Reporter reporter) throws IOException {
			return new RandomRecordReader(((FileSplit) split).getPath());
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

	static class MapClient extends MapReduceBase
			implements Mapper<Object, Text, Text, Text>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value,
						OutputCollector<Text, Text> out,
						Reporter reporter) throws IOException {
			//To change body of implemented methods use File | Settings | File Templates.
			out.collect(new Text(getLocalIP()), new Text("a"));
		}
	}

//	static class Map extends MapReduceBase
//			implements org.apache.hadoop.mapred.Mapper<WritableComparable, Writable,
//			BytesWritable, BytesWritable> {
//
//		private long numBytesToWrite;
//		private int minKeySize;
//		private int keySizeRange;
//		private int minValueSize;
//		private int valueSizeRange;
//		private Random random = new Random();
//		private BytesWritable randomKey = new BytesWritable();
//		private BytesWritable randomValue = new BytesWritable();
//
//		private void randomizeBytes(byte[] data, int offset, int length) {
//			for(int i=offset + length - 1; i >= offset; --i) {
//				data[i] = (byte) random.nextInt(256);
//			}
//		}
//
//		/**
//		 * Given an output filename, write a bunch of random records to it.
//		 */
//		public void map(WritableComparable key,
//						Writable value,
//						OutputCollector<BytesWritable, BytesWritable> output,
//						Reporter reporter) throws IOException {
//			int itemCount = 0;
//			while (numBytesToWrite > 0) {
//				int keyLength = minKeySize +
//						(keySizeRange != 0 ? random.nextInt(keySizeRange) : 0);
//				randomKey.setSize(keyLength);
//				randomizeBytes(randomKey.getBytes(), 0, randomKey.getLength());
//				int valueLength = minValueSize +
//						(valueSizeRange != 0 ? random.nextInt(valueSizeRange) : 0);
//				randomValue.setSize(valueLength);
//				randomizeBytes(randomValue.getBytes(), 0, randomValue.getLength());
//				output.collect(randomKey, randomValue);
//				numBytesToWrite -= keyLength + valueLength;
//				if (++itemCount % 200 == 0) {
//					reporter.setStatus("wrote record " + itemCount + ". " +
//							numBytesToWrite + " bytes left.");
//				}
//			}
//			reporter.setStatus("done with " + itemCount + " records.");
//		}
//
//		/**
//		 * Save the values out of the configuaration that we need to write
//		 * the data.
//		 */
//		@Override
//		public void configure(JobConf job) {
//			numBytesToWrite = job.getLong("test.randomwrite.bytes_per_map",
//					1*1024*1024*1024);
//			minKeySize = job.getInt("test.randomwrite.min_key", 10);
//			keySizeRange =
//					job.getInt("test.randomwrite.max_key", 1000) - minKeySize;
//			minValueSize = job.getInt("test.randomwrite.min_value", 0);
//			valueSizeRange =
//					job.getInt("test.randomwrite.max_value", 20000) - minValueSize;
//		}
//
//	}

	public int run(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("Usage: SIMR <out-dir>");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Path outDir = new Path(args[0]);
		JobConf job = new JobConf(getConf());

		job.setJarByClass(SIMR.class);
		job.setJobName("new SIMR");
		org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(job, outDir);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormat(RandomInputFormat.class);
		job.setMapperClass(MapClient.class);
//		job.setReducerClass(IdentityReducer.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		JobClient client = new JobClient(job);
		ClusterStatus cluster = client.getClusterStatus();
		Integer clusterSize = cluster.getTaskTrackers();


		// reducer NONE
		job.setNumReduceTasks(0);

		Date startTime = new Date();
		System.out.println("Job started: " + startTime + " with " + clusterSize + " task trackers");
		job.set("clustersize", clusterSize.toString());
		JobClient.runJob(job);
		Date endTime = new Date();
		System.out.println("Job ended: " + endTime);
		System.out.println("The job took " +
				(endTime.getTime() - startTime.getTime()) /1000 +
				" seconds.");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SIMR(), args);
		System.exit(res);
	}

}

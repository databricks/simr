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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SIMR {

	static class RandomInputFormat implements InputFormat<Text, Text> {

		/**
		 * Generate the requested number of file splits, with the filename
		 * set to the filename of the output file.
		 */
		public InputSplit[] getSplits(JobConf job,
									  int numSplits_) throws IOException {
			int numSplits = new JobClient().getClusterStatus().getTaskTrackers();
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

    public static class TokenizerMapper 
	extends Mapper<Object, Text, Text, IntWritable>{
    
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
      
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
	    context.write(new Text(getLocalIP()), new IntWritable(1));
	//     StringTokenizer itr = new StringTokenizer(value.toString());
	//     while (itr.hasMoreTokens()) {
	// 	word.set(itr.nextToken());
	// 	context.write(word, one);
	//     }
        }
    }
  
    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	if (otherArgs.length != 2) {
	    System.err.println("Usage: SIMR <in> <out>");
	    System.exit(2);
	}
	Job job = new Job(conf, "SIMR");

	job.setNumReduceTasks(0);
	job.setJarByClass(SIMR.class);
	job.setMapperClass(TokenizerMapper.class);
	// job.setCombinerClass(IntSumReducer.class);
	// job.setReducerClass(IntSumReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

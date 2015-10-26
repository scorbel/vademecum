package pagerank;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RemoveDeadends {

	enum myCounters {
		NUMNODES;
	}

	static class Map extends Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String values = value.toString();
			String[] tokens = values.split("\t");
			context.write(new Text(tokens[0]), new Text("S " + tokens[1]));
			context.write(new Text(tokens[1]), new Text("P " + tokens[0]));
		}
	}

	static class Reduce extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<Integer> sList = new ArrayList<Integer>();
			ArrayList<Integer> pList = new ArrayList<Integer>();
			for (Text value : values) {
				String s = value.toString();
				String[] tokens = s.split("\\s");
				Integer node = Integer.parseInt(tokens[1]);
				if (tokens[0].equals("P")) {
					pList.add(node);
				} else {
					sList.add(node);
				}
			}
			if (sList.size() != 0) {
				context.getCounter(myCounters.NUMNODES).increment(1);
				for (Integer n : pList) {
					context.write(new Text(Integer.toString(n)), key);
				}
			}

		}
	}

	public static void job(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

		boolean existDeadends = true;

		/*
		 * You don't need to use or create other folders besides the two listed
		 * below. In the beginning, the initial graph is copied in the
		 * processedGraph. After this, the working directories are
		 * processedGraphPath and intermediaryResultPath. The final output
		 * should be in processedGraphPath.
		 */

		FileUtils.copyDirectory(new File(conf.get("graphPath")), new File(conf.get("processedGraphPath")));
		String intermediaryDir = conf.get("intermediaryResultPath");
		String currentInput = conf.get("processedGraphPath");

		long nNodes = conf.getLong("numNodes", 0);

		while (existDeadends) {
			Job job = Job.getInstance(conf);
			job.setJobName("deadends job");
			/*
			 * TO DO : configure job and move in the best manner the output for
			 * each iteration you have to update the number of nodes in the
			 * graph after each iteration, use conf.setLong("numNodes", nNodes);
			 */
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.setInputPaths(job, new Path[] { new Path(currentInput) });
			FileOutputFormat.setOutputPath(job, new Path(intermediaryDir));

			job.waitForCompletion(true);

			FileUtils.deleteQuietly(new File(currentInput));
			FileUtils.copyDirectory(new File(intermediaryDir), new File(currentInput));
			FileUtils.deleteQuietly(new File(intermediaryDir));

			Counters counters = job.getCounters();
			Counter c1 = counters.findCounter(myCounters.NUMNODES);
			existDeadends = (c1.getValue() != nNodes);
			nNodes = c1.getValue();

		}
		conf.setLong("numNodes", nNodes);
		// when you finished implementing delete this line
		FileUtils.deleteQuietly(new File(conf.get("intermediaryResultPath")));
		// throw new UnsupportedOperationException("Implementation missing");

	}

}
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BFS
{
	public static class ReadDataMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Text nodeA = new Text();
			Text nodeB = new Text();
			String line = value.toString();
			String[] tsv = line.split("\t");
			nodeA.set(tsv[0]);
			nodeB.set(tsv[1]);

			context.write(nodeA, nodeB);
			context.write(nodeB, nodeA);
		}
	}

	public static class ReadDataReducer extends Reducer<Text, Text, Text, Text>
	{
		private static String startNode = null;
		protected void setup(Context context) throws IOException, InterruptedException
		{
			startNode = context.getConfiguration().get("startNode");
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Text node = new Text(); 
			StringBuilder info = new StringBuilder();

			if (key.toString().equals(startNode))
				info.append("0\t");
			else
				info.append("I\t");

			node.set(key.toString());
			for (Text value : values)
				info.append(value.toString() + ",");
			info.deleteCharAt(info.length() - 1);
			context.write(node, new Text(info.toString()));
		}
	}

	public static class BFSMapper extends Mapper<Object, Text, Text, Text>
	{
		private static final String BFS_STAT_GROUP = "BFS";
		private static final String UNVISITED = "unvisited";

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] tsv = line.split("\t");

			String node = tsv[0];
			String dist = tsv[1];
			String[] nodes = tsv[2].split(",");
			String newDist = "I";

			if (!dist.equals("I"))
				newDist = Integer.toString(Integer.parseInt(dist) + 1);
			else
				context.getCounter(BFS_STAT_GROUP, UNVISITED).increment(1);

			for (int i = 0; i < nodes.length; i++)
				context.write(new Text(nodes[i]), new Text("D" + newDist));

			context.write(new Text(node), new Text("D" + dist));
			context.write(new Text(node), new Text("N" + tsv[2]));
		}
	}

	public static class BFSReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int minDist = Integer.MAX_VALUE;
			String nodes = null;

			for (Text value : values)
			{
				if (value.toString().charAt(0) == 'N')
					nodes = value.toString().substring(1);
				else
				{
					int dist = Integer.MAX_VALUE;
					if (!value.toString().substring(1).equals("I"))
						dist = Integer.parseInt(value.toString().substring(1));
					minDist = Math.min(minDist, dist);
				}
			}

			String newDist = minDist == Integer.MAX_VALUE ? "I" : Integer.toString(minDist);
			context.write(key, new Text(newDist + "\t" + nodes));
		}
	}

	public static class OutputMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] tsv = line.split("\t");
			Text node = new Text();
			Text dist = new Text();
			node.set(tsv[0]);
			dist.set(tsv[1]);

			context.write(dist, node);
		}
	}

	public static class OutputReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			for (Text value : values)
				context.write(new Text(value), new Text(key));
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: BFS <input> <output>");
			System.exit(2);
		}

		String inputPath = otherArgs[0];
		String tempPath1 = "temp1";
		String tempPath2 = "temp2";
		String outputPath = otherArgs[1];

		conf.set("startNode", "s");

		Job job = Job.getInstance(conf, "ReadData");
		job.setJarByClass(BFS.class);
		job.setMapperClass(ReadDataMapper.class);
		job.setReducerClass(ReadDataReducer.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(tempPath1));
		int code = job.waitForCompletion(true) ? 0 : 1;
	
		long numUnvisited = Long.MAX_VALUE;

		while (code == 0 && numUnvisited != 0)
		{
			Job bfsJob = Job.getInstance(conf, "BFS");
			bfsJob.setJarByClass(BFS.class);
			bfsJob.setMapperClass(BFSMapper.class);
			bfsJob.setReducerClass(BFSReducer.class);
			bfsJob.setNumReduceTasks(2);
			bfsJob.setOutputKeyClass(Text.class);
			bfsJob.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(bfsJob, new Path(tempPath1));
			FileOutputFormat.setOutputPath(bfsJob, new Path(tempPath2));
			code = bfsJob.waitForCompletion(true) ? 0 : 1;

			FileSystem.get(conf).delete(new Path(tempPath1), true);
			numUnvisited = bfsJob.getCounters().findCounter("BFS", "unvisited").getValue();

			String temp = new String(tempPath1);
			tempPath1 = tempPath2;
			tempPath2 = temp;
		}

		Job outJob = Job.getInstance(conf, "Output");
		outJob.setJarByClass(BFS.class);
		outJob.setMapperClass(OutputMapper.class);
		outJob.setReducerClass(OutputReducer.class);
		outJob.setNumReduceTasks(1);
		outJob.setOutputKeyClass(Text.class);
		outJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(outJob, new Path(tempPath1));
		FileOutputFormat.setOutputPath(outJob, new Path(outputPath));
		code = outJob.waitForCompletion(true) ? 0 : 1;		

		System.exit(code);
	}
}

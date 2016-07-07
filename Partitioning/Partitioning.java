import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Partitioning
{
	public static class CountMapper extends Mapper<Object, Text, NullWritable, NullWritable>
	{
		public static final String COUNTER_GROUP = "counter";
		public static final String UNIQ_DIST = "uniqDist";
		public static Set<String> set = null; 

		protected void setup(Context context) throws IOException, InterruptedException
		{
			set = new HashSet<String>();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] tsv = line.split("\t");
			String node = tsv[0];
			String dist = tsv[1];

			if (!set.contains(dist))
			{
				set.add(dist);
				context.getCounter(COUNTER_GROUP, UNIQ_DIST).increment(1);
			}

		}
	}

	public static class PartitionMapper extends Mapper<Object, Text, IntWritable, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] tsv = line.split("\t");
			String node = tsv[0];
			String dist = tsv[1];

			context.write(new IntWritable(Integer.parseInt(dist)), new Text(value));
		}
	}

	public static class PartitionReducer extends Reducer<IntWritable, Text, Text, NullWritable>
	{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			for (Text val : values)
				context.write(val, NullWritable.get());
		}
	}

	public static class MyPartitioner extends Partitioner<IntWritable, Text>
	{
		public int getPartition(IntWritable key, Text value, int numPartitions)
		{
			return key.get();
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: Partition <int> <out>");
			System.exit(2);
		}

		String inputPath = otherArgs[0];
		String outputPath = otherArgs[1];

		Job countJob = Job.getInstance(conf, "Count");
		countJob.setJarByClass(Partitioning.class);
		countJob.setMapperClass(CountMapper.class);
		countJob.setNumReduceTasks(0);
		countJob.setOutputKeyClass(NullWritable.class);
		countJob.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(countJob, new Path(inputPath));
		FileOutputFormat.setOutputPath(countJob, new Path(outputPath));
		countJob.waitForCompletion(true);

		long uniqDist = countJob.getCounters().findCounter("counter", "uniqDist").getValue();
		FileSystem.get(conf).delete(new Path(outputPath), true);

		Job partitionJob = Job.getInstance(conf, "Parition");
		partitionJob.setJarByClass(Partitioning.class);
		partitionJob.setMapperClass(PartitionMapper.class);
		partitionJob.setPartitionerClass(MyPartitioner.class);
		partitionJob.setReducerClass(PartitionReducer.class);
		partitionJob.setNumReduceTasks((int) uniqDist);

		partitionJob.setOutputKeyClass(Text.class);
		partitionJob.setOutputValueClass(NullWritable.class);
		partitionJob.setMapOutputKeyClass(IntWritable.class);
		partitionJob.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(partitionJob, new Path(inputPath));
		FileOutputFormat.setOutputPath(partitionJob, new Path(outputPath));

		System.exit(partitionJob.waitForCompletion(true) ? 0 : 1);
	}
}

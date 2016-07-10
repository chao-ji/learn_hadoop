import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class KMeans
{
	public static class AssignDataMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] tsv = value.toString().split("\t");
			String id = tsv[0];
			
			context.write(new Text(id), new Text("D" + value.toString()));
		}
	}

	public static class AssignCenterMapper extends Mapper<Object, Text, Text, Text>
	{
		private int size;
		protected void setup(Context context) throws IOException, InterruptedException
		{
			size = Integer.parseInt(context.getConfiguration().get("size"));
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			for (int i = 0; i < size; i++)
				context.write(new Text(Integer.toString(i)), new Text("C" + value.toString()));
		}
	}

	public static class AssignReducer extends Reducer<Text, Text, Text, NullWritable>
	{
		private static final String KMEANS_STAT_GROUP = "KMeans";
		private static final String UNCHANGED = "unchanged";

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			List<String> centers = new ArrayList<String>();
			String vector = null;
			for (Text val : values)
				if (val.toString().charAt(0) == 'D')
					vector = val.toString().substring(1);
				else
					centers.add(val.toString().substring(1));
			
			String[] tsv1 = vector.split("\t");
			String id = tsv1[0];
			String partition = tsv1[1];
			int dim = tsv1.length - 2;
			double[] list1 = new double[dim];
			for (int i = 2; i < tsv1.length; i++)
				list1[i - 2] = Double.parseDouble(tsv1[i]);
	
			String index = null;
			double min = Double.MAX_VALUE;

			for (int i = 0; i < centers.size(); i++)
			{
				String[] tsv2 = centers.get(i).split("\t");
				double[] list2 = new double[dim];
				for (int j = 1; j < tsv2.length; j++)
					list2[j - 1] = Double.parseDouble(tsv2[j]);

				double dist = euclideanDist(list1, list2);
				if (dist < min)
				{
					index = tsv2[0];
					min = dist;
				}
			}

			if (tsv1[1].equals(index))
				context.getCounter(KMEANS_STAT_GROUP, UNCHANGED).increment(1);
			tsv1[1] = index;
			context.write(new Text(String.join("\t", tsv1)), NullWritable.get());
		}
	}

	public static class UpdateMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] tsv = line.split("\t");
			String partition = tsv[1];

			StringBuilder out = new StringBuilder();
			for (int i = 2; i < tsv.length; i++)
					out.append(tsv[i] + "\t");

			context.write(new Text(partition), new Text(out.toString() + "1"));
		}
	}

	public static class UpdateCombiner extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			double[] sum = null;
			int count = 0;

			for (Text val : values)
			{
				String[] tsv = val.toString().split("\t");
				int dim = tsv.length - 1;
				int size = Integer.parseInt(tsv[tsv.length - 1]);
				
				if (sum == null)
					sum = new double[dim];
				for (int i = 0; i < tsv.length - 1; i++)
					sum[i] += Double.parseDouble(tsv[i]);
				count += size;
			}
		
			StringBuilder out = new StringBuilder();
			for (int i = 0; i < sum.length; i++)
				out.append(Double.toString(sum[i]) + "\t");
			out.append(Integer.toString(count));

			context.write(key, new Text(out.toString()));
		}
	}

	public static class UpdateReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			double[] sum = null;
			int count = 0;

			for (Text val : values)
			{
				String[] tsv = val.toString().split("\t");
				int dim = tsv.length - 1;
				int size = Integer.parseInt(tsv[tsv.length - 1]);

				if (sum == null)
					sum = new double[dim];
				for (int i = 0; i < tsv.length - 1; i++)
					sum[i] += Double.parseDouble(tsv[i]);
				count += size;
			}

			StringBuilder out = new StringBuilder();
			for (int i = 0; i < sum.length; i++)
				if (i < sum.length - 1)
					out.append(Double.toString(sum[i] / count) + "\t");
				else
					out.append(Double.toString(sum[i] / count));
			context.write(key, new Text(out.toString()));
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4)
		{
			System.err.println("Usage: KMeans <data> <center> <size> <max>");
			System.exit(2);
		}

		long numUnchanged = Long.MAX_VALUE;
		String data = otherArgs[0];
		String center = otherArgs[1];

		String center1 = center + "_1";
		String data1 = data + "_1";
		String center2 = center + "_2";
		String data2 = data + "_2";

		conf.set("size", otherArgs[2]);
		int max = Integer.parseInt(otherArgs[3]);

		Job updateJob = Job.getInstance(conf, "KMeans Update");
		updateJob.setJarByClass(KMeans.class);
		updateJob.setMapperClass(UpdateMapper.class);
		updateJob.setCombinerClass(UpdateCombiner.class);
		updateJob.setReducerClass(UpdateReducer.class);
		updateJob.setNumReduceTasks(1);
		updateJob.setOutputKeyClass(Text.class);
		updateJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(updateJob, new Path(data));
		FileOutputFormat.setOutputPath(updateJob, new Path(center1));
		updateJob.waitForCompletion(true);
	
		Job assignJob = Job.getInstance(conf, "KMeans Assignment");
		assignJob.setJarByClass(KMeans.class);
		MultipleInputs.addInputPath(assignJob, new Path(data), TextInputFormat.class, AssignDataMapper.class);
		MultipleInputs.addInputPath(assignJob, new Path(center1), TextInputFormat.class, AssignCenterMapper.class);
		assignJob.setReducerClass(AssignReducer.class);
		assignJob.setNumReduceTasks(1);
		assignJob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(assignJob, new Path(data1));
		assignJob.setMapOutputKeyClass(Text.class);
		assignJob.setMapOutputValueClass(Text.class);
		assignJob.setOutputKeyClass(Text.class);
		assignJob.setOutputValueClass(NullWritable.class);
		assignJob.waitForCompletion(true);

		for (int i = 0; i < max && numUnchanged != 0; i++)
		{
			updateJob = Job.getInstance(conf, "KMeans Update");
			updateJob.setJarByClass(KMeans.class);
			updateJob.setMapperClass(UpdateMapper.class);
			updateJob.setCombinerClass(UpdateCombiner.class);
			updateJob.setReducerClass(UpdateReducer.class);
			updateJob.setNumReduceTasks(1);
			updateJob.setOutputKeyClass(Text.class);
			updateJob.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(updateJob, new Path(data1));
			FileOutputFormat.setOutputPath(updateJob, new Path(center2));
			updateJob.waitForCompletion(true);

			assignJob = Job.getInstance(conf, "KMeans Assignment");
			assignJob.setJarByClass(KMeans.class);
			MultipleInputs.addInputPath(assignJob, new Path(data1), TextInputFormat.class, AssignDataMapper.class);
			MultipleInputs.addInputPath(assignJob, new Path(center2), TextInputFormat.class, AssignCenterMapper.class);
			assignJob.setReducerClass(AssignReducer.class);
			assignJob.setNumReduceTasks(1);
			assignJob.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(assignJob, new Path(data2));
			assignJob.setMapOutputKeyClass(Text.class);
			assignJob.setMapOutputValueClass(Text.class);
			assignJob.setOutputKeyClass(Text.class);
			assignJob.setOutputValueClass(NullWritable.class);
			assignJob.waitForCompletion(true);

			FileSystem.get(conf).delete(new Path(data1), true);
			FileSystem.get(conf).delete(new Path(center1), true);

			numUnchanged = assignJob.getCounters().findCounter("Kmeans", "unchanged").getValue(); 

			String temp = new String(data1);
			data1 = data2;
			data2 = temp;

			temp = new String(center1);
			center1 = center2;
			center2 = temp;
		}
	}

	public static double euclideanDist(double[] vector1, double[] vector2)
	{
		double dist = 0.0;
		for (int i = 0; i < vector1.length; i++)
			dist += (vector1[i] - vector2[i]) * (vector1[i] - vector2[i]);

		return Math.sqrt(dist);
	}
}

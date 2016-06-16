import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NaiveBayesTrain
{
	public static class MeanMapper extends Mapper<Object, Text, Text, SumCountTuple>
	{
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] parts = line.split("\t");

			String label = parts[parts.length - 1];
			for (int i = 0; i < parts.length - 1; i++)
			{
				SumCountTuple tuple = new SumCountTuple();
				
				Text outputKey = new Text();
				outputKey.set(Integer.toString(i) + "_" + label);

				tuple.setSum(Double.parseDouble(parts[i]));
				tuple.setCount(one.get());
				context.write(outputKey, tuple);
			}
		}
	}

	public static class MeanCombiner extends Reducer<Text, SumCountTuple, Text, SumCountTuple>
	{
		private SumCountTuple tuple = new SumCountTuple();

		public void reduce(Text key, Iterable<SumCountTuple> values, Context context) throws IOException, InterruptedException
		{
			double sum = 0.0;
			int count = 0;
			for (SumCountTuple val : values)
			{
				sum += val.getSum();
				count += val.getCount();
			}
			tuple.setSum(sum);
			tuple.setCount(count);
			context.write(key, tuple);
		}
	}

	public static class MeanReducer extends Reducer<Text, SumCountTuple, Text, Text>
	{
		Text result = new Text();
		public void reduce(Text key, Iterable<SumCountTuple> values, Context context) throws IOException, InterruptedException
		{
			double sum = 0.0;
			int count = 0;
			double mean = 0.0;
			for (SumCountTuple val : values)
			{
				sum += val.getSum();
				count += val.getCount();
			}
			mean = sum / count;
			result.set(Double.toString(mean) + ":" + Integer.toString(count));
			context.write(key, result);
		}
	}

	public static class SumSquareMapper extends Mapper<Object, Text, Text, DoubleWritable>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] parts = line.split("\t");

			String label = parts[parts.length - 1];
			for (int i = 0; i < parts.length - 1; i++)
			{
				Text outputKey = new Text();
				outputKey.set(Integer.toString(i) + "_" + label);
				double val = Double.parseDouble(parts[i]);
				context.write(outputKey, new DoubleWritable(val * val));
			}
		}
	}

	public static class SumSquareReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
	{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
		{
			double sum = 0;
			for (DoubleWritable value : values)
				sum += value.get();
			context.write(key, new DoubleWritable(sum));
		}
	}

	public static class VarianceMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] parts = line.split("\t");
			context.write(new Text(parts[0]), new Text(parts[1]));
		}
	}

	public static class VarianceReducer extends Reducer<Text, Text, Text, Text>
	{
		Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			double mean = 0.0;
			int count = 0;
			double sumsquare = 0.0;
			double var = 0.0;

			for (Text value : values)
			{
				String content = value.toString();
				String[] parts = content.split(":");
				if (parts.length == 1)
					sumsquare = Double.parseDouble(parts[0]);
				else
				{
					mean = Double.parseDouble(parts[0]);
					count = Integer.parseInt(parts[1]);
				}
			}

			var = (sumsquare - count * mean * mean) / (count - 1);
			result.set("mean = " + Double.toString(mean) + "\t" + "var = " + Double.toString(var));
			context.write(key, result);
		}
	}

	public static class PriorMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] parts = line.split("\t");
			String[] list1 = parts[0].split("_");
			String[] list2 = parts[1].split(":");

			if (list1[0].equals("0"))
			{
				String label = list1[1];
				String count = list2[1];
				context.write(new Text("key"), new Text(label + ":" + count));
			}
		}
	}

	public static class PriorReducer extends Reducer<Text, Text, Text, DoubleWritable>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int total = 0;
			List<Integer> counts = new ArrayList<Integer>();
			List<String> labels = new ArrayList<String>();

			for (Text val : values)
			{
				String[] parts = val.toString().split(":");
				total += Integer.parseInt(parts[1]);
				counts.add(Integer.parseInt(parts[1]));
				labels.add(parts[0]);
			}

			for (int i = 0; i < counts.size(); i++)
			{
				double outputValue = (double) counts.get(i) / total;
				context.write(new Text(labels.get(i)), new DoubleWritable(outputValue));
			}	
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: NaiveBayesTrain <in> <out>");
			System.exit(2);
		}

		// Configure job that computes mean
		Job job = new Job(conf, "Mean");
		job.setJarByClass(NaiveBayesTrain.class);
		job.setMapperClass(MeanMapper.class);
		job.setCombinerClass(MeanCombiner.class);
		job.setReducerClass(MeanReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SumCountTuple.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "_mean"));
		job.waitForCompletion(true);

		// Configure job that computes sum of squares
		job = new Job(conf, "Square");
		job.setJarByClass(NaiveBayesTrain.class);
		job.setMapperClass(SumSquareMapper.class);
		job.setCombinerClass(SumSquareReducer.class);
		job.setReducerClass(SumSquareReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "_sumsquare"));
		job.waitForCompletion(true);

		// Configure job that computes variance
		job = new Job(conf, "Variance");
		job.setJarByClass(NaiveBayesTrain.class);
		job.setMapperClass(VarianceMapper.class);
		job.setReducerClass(VarianceReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1] + "_mean"));
		FileInputFormat.addInputPath(job, new Path(otherArgs[1] + "_sumsquare"));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "_var"));
		job.waitForCompletion(true);

    // Configure job that computes class prior
		job = new Job(conf, "Prior");
		job.setJarByClass(NaiveBayesTrain.class);
		job.setMapperClass(PriorMapper.class);
		job.setReducerClass(PriorReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1] + "_mean"));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "_prior"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class SumCountTuple implements Writable
	{
		private double sum = 0.0;
		private int count = 0;

		public Double getSum() { return sum; }

		public void setSum(Double sum) { this.sum = sum; }

		public Integer getCount() { return count; }

		public void setCount(Integer count) { this.count = count; }

		public void readFields(DataInput in) throws IOException
		{
			sum = in.readDouble();
			count = in.readInt();
		}

		public void write(DataOutput out) throws IOException
		{
			out.writeDouble(sum);
			out.writeInt(count);
		}
	}
}

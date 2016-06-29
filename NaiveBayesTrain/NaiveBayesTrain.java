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
	public static class MeanMapper extends Mapper<Object, Text, Text, MeanCountTuple>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] tsv = line.split("\t");

			String classID = tsv[tsv.length - 1];
			for (int i = 0; i < tsv.length - 1; i++)
			{
				String featureID = Integer.toString(i);
				MeanCountTuple tuple = new MeanCountTuple();
				Text outputKey = new Text();

				outputKey.set(featureID + ":" + classID);
				tuple.setMean(Double.parseDouble(tsv[i]));
				tuple.setCount(1);
				context.write(outputKey, tuple);
			}
		}
	}

	public static class MeanReducer extends Reducer<Text, MeanCountTuple, Text, MeanCountTuple>
	{
		private MeanCountTuple result = new MeanCountTuple();

		public void reduce(Text key, Iterable<MeanCountTuple> values, Context context) throws IOException, InterruptedException
		{
			int count = 0;
			double sum = 0.0;
			for (MeanCountTuple val : values)
			{
				sum += val.getMean() * val.getCount();
				count += val.getCount();
			}
			result.setMean(sum / count);
			result.setCount(count);
			context.write(key, result);
		}
	}


	public static class SumSquareMapper extends Mapper<Object, Text, Text, DoubleWritable>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] tsv = line.split("\t");

			String classID = tsv[tsv.length - 1];
			for (int i = 0; i < tsv.length - 1; i++)
			{
				String featureID = Integer.toString(i);
				Text outputKey = new Text();
				outputKey.set(featureID + ":" + classID);
				double val = Double.parseDouble(tsv[i]);
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
			String[] tsv = line.split("\t");
			Text outputKey = new Text(tsv[0]);
			Text outputVal = new Text();
			if (tsv.length == 2)
				outputVal.set(tsv[1]);
			else if (tsv.length == 3)
				outputVal.set(tsv[1] + "\t" + tsv[2]);

			context.write(outputKey, outputVal);
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

			for (Text val : values)
			{
				String[] tsv = val.toString().split("\t");
				if (tsv.length == 1)
					sumsquare = Double.parseDouble(tsv[0]);
				else
				{
					mean = Double.parseDouble(tsv[0]);
					count = Integer.parseInt(tsv[1]);
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
			String[] tsv = line.split("\t");
			String[] list = tsv[0].split(":");

			if (list[0].equals("0"))
			{
				String classID = list[1];
				String count = tsv[2];
				context.write(new Text("key"), new Text(classID + ":" + count));
			}
		}
	}

	public static class PriorReducer extends Reducer<Text, Text, Text, DoubleWritable>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int total = 0;
			List<Integer> counts = new ArrayList<Integer>();
			List<String> classID = new ArrayList<String>();

			for (Text val : values)
			{
				String[] tsv = val.toString().split(":");
				total += Integer.parseInt(tsv[1]);
				counts.add(Integer.parseInt(tsv[1]));
				classID.add(tsv[0]);
			}

			for (int i = 0; i < counts.size(); i++)
			{
				double outputVal = (double) counts.get(i) / total;
				context.write(new Text(classID.get(i)), new DoubleWritable(outputVal));
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
		job.setCombinerClass(MeanReducer.class);
		job.setReducerClass(MeanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MeanCountTuple.class);
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

	public static class MeanCountTuple implements Writable
	{
		private double mean = 0.0;
		private int count = 0;

		public double getMean() { return mean; }

		public void setMean(double mean) { this.mean = mean; }

		public int getCount() { return count; }

		public void setCount(int count) { this.count = count; }

		public void readFields(DataInput in) throws IOException
		{
			mean = in.readDouble();
			count = in.readInt();
		}

		public void write(DataOutput out) throws IOException
		{
			out.writeDouble(mean);
			out.writeInt(count);
		}

		public String toString()
		{
			return Double.toString(mean) + "\t" + Integer.toString(count);
		}
	}
}

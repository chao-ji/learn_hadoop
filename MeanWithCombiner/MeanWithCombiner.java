import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

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

public class MeanWithCombiner
{
	public static class MeanMapper extends Mapper<Object, Text, Text, MeanCountTuple>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			MeanCountTuple tuple = new MeanCountTuple();
			tuple.setMean(Double.parseDouble(value.toString()));
			tuple.setCount(1);
			context.write(new Text("key"), tuple);
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

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: MeanWithCombiner <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "MeanWithCombiner");
		job.setJarByClass(MeanWithCombiner.class);
		job.setMapperClass(MeanMapper.class);
		job.setCombinerClass(MeanReducer.class);
		job.setReducerClass(MeanReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MeanCountTuple.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
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

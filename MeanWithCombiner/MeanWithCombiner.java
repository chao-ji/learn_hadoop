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
	public static class MeanMapper extends Mapper<Object, Text, Text, SumCountTuple>
	{
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			SumCountTuple tuple = new SumCountTuple();
			tuple.setSum(Double.parseDouble(value.toString()));
			tuple.setCount(one.get());
			context.write(new Text("key"), tuple);
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

	public static class MeanReducer extends Reducer<Text, SumCountTuple, Text, DoubleWritable>
	{
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
			context.write(key, new DoubleWritable(mean));
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
		job.setCombinerClass(MeanCombiner.class);
		job.setReducerClass(MeanReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SumCountTuple.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
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

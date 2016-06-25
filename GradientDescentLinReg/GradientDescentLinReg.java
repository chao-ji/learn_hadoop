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

public class GradientDescentLinReg
{
	public static class UpdateMapper1 extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] parts = line.split("\t");

			if (parts[0].charAt(0) == 'D')
			{
				String label = parts[0].substring(1);
				for (int i = 1; i < parts.length - 1; i++)
					context.write(new Text(Integer.toString(i)), new Text(parts[i] + ":" + label));

				context.write(new Text("Y"), new Text(parts[parts.length - 1] + ":" + label));
			}
			else
			{
				for (int i = 1; i < parts.length; i++)
					context.write(new Text(Integer.toString(i)), new Text(parts[i] + ":ALPHA"));
			}
		}
	}


	public static class UpdateReducer1 extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			if (key.toString().equals("Y"))
			{
				for (Text val : values)
				{
					String[] parts = val.toString().split(":");
					context.write(new Text(parts[1]), new Text(parts[0] + ":Y"));
				}
			}
			else
			{
				List<String> nums = new ArrayList<String>();
				List<String> index = new ArrayList<String>();
				String alpha = null;
				for (Text val : values)
				{
					String[] parts = val.toString().split(":");
					if (parts[1].equals("ALPHA"))
						alpha = parts[0];
					else
					{
						nums.add(parts[0]);
						index.add(parts[1]);
					}
				}

				for (int i = 0; i < index.size(); i++)
				{
					double val = Double.parseDouble(nums.get(i)) * Double.parseDouble(alpha);
					context.write(new Text(index.get(i)), new Text(Double.toString(val) + ":XA"));		
				}
			}
		}
	}

	public static class UpdateMapper2 extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] parts = line.split("\t");
			context.write(new Text(parts[0]), new Text(parts[1]));
		}
	}

	public static class UpdateReducer2 extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			List<String> list = new ArrayList<String>();
			String y = null;
			for (Text val : values)
			{
				String[] parts = val.toString().split(":");
				if (parts[1].equals("XA"))
					list.add(parts[0]);
				else
					y = parts[0];
			}
	
			double error = Double.parseDouble(y);
			for (int i = 0; i < list.size(); i++)
				error -= Double.parseDouble(list.get(i));

			context.write(key, new Text(Double.toString(error)));	
		}	
	}

	public static class UpdateMapper3 extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] parts = line.split("\t");
			
			if (parts[0].charAt(0) == 'D')
			{
				String outputKey = parts[0].substring(1);
				for (int i = 1; i < parts.length - 1; i++)
					context.write(new Text(outputKey), new Text(parts[i] + ":" + Integer.toString(i)));
			}
			else
				context.write(new Text(parts[0]), new Text(parts[1] + ":E"));
		}
	}

	public static class UpdateReducer3 extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			List<String> nums = new ArrayList<String>();
			List<String> index = new ArrayList<String>();
			String error = null;
			for (Text val : values)
			{
				String[] parts = val.toString().split(":");
				if (parts[1].equals("E"))
					error = parts[0];
				else
				{
					nums.add(parts[0]);
					index.add(parts[1]);
				}
			}

			for (int i = 0; i < index.size(); i++)
			{
				double val = Double.parseDouble(nums.get(i)) * Double.parseDouble(error);
				context.write(new Text(index.get(i)), new Text(Double.toString(val)));
			}
		}
	}

	public static class UpdateMapper4 extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] parts = line.split("\t");

			context.write(new Text(parts[0]), new Text(parts[1]));
		}
	}

	public static class UpdateReducer4 extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			double sum = 0.0;
			for (Text val : values)
				sum += Double.parseDouble(val.toString());
			context.write(key, new Text(Double.toString(sum)));
		}
	}

	public static class UpdateMapper5 extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] parts = line.split("\t");

			if (parts[0].equals("ALPHA"))
			{
				for (int i = 1; i < parts.length; i++)
					context.write(new Text(Integer.toString(i)), new Text(parts[i] + ":A"));	
			}
			else
				context.write(new Text(parts[0]), new Text(parts[1] + ":E"));
		}
	}

	public static class UpdateReducer5 extends Reducer<Text, Text, Text, Text>
	{
		private final double learningRate = 0.01;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			double updatedAlpha = 0.0;
			double alpha = 0.0;
			double error = 0.0;

			for (Text val : values)
			{
				String[] parts = val.toString().split(":");
				if (parts[1].equals("E"))
					error = Double.parseDouble(parts[0]);
				else
					alpha = Double.parseDouble(parts[0]);
			}

			updatedAlpha = alpha + learningRate * error;
			context.write(new Text("ALPHA"), new Text(Double.toString(updatedAlpha) + ":" + key.toString()));
		}
	}


	public static class UpdateMapper6 extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] parts = line.split("\t");

			context.write(new Text(parts[0]), new Text(parts[1]));
		}
	}

	public static class UpdateReducer6 extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			List<String> nums = new ArrayList<String>();
			KeyComparator kc = new KeyComparator();

			for (Text val : values)
				nums.add(val.toString());

			Collections.sort(nums, kc);
			StringBuilder updatedAlpha = new StringBuilder();

			for (int i = 0; i < nums.size(); i++)
			{
				String[] parts = nums.get(i).split(":");
				updatedAlpha.append(parts[0] + "\t");
			}

			updatedAlpha.deleteCharAt(updatedAlpha.length() - 1);
			context.write(key, new Text(updatedAlpha.toString()));
		}
	}

	static class KeyComparator implements Comparator<String>
	{
		public int compare(String s1, String s2)
		{
			String[] parts1 = s1.split(":");
			String[] parts2 = s2.split(":");
			return Integer.parseInt(parts1[1]) - Integer.parseInt(parts2[1]);
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

		String input = otherArgs[0];
		String output = otherArgs[1];
		Job job = null;
		int iterations = 2;

		for (int i = 0; i < iterations; i++)
		{
			job = new Job(conf, "Update1");
			job.setJarByClass(GradientDescentLinReg.class);
			job.setMapperClass(UpdateMapper1.class);
			job.setReducerClass(UpdateReducer1.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(input + "_alpha_" + Integer.toString(i)));
			FileInputFormat.addInputPath(job, new Path(input + "_data"));
			FileOutputFormat.setOutputPath(job, new Path("step_0_" + Integer.toString(i)));
			job.waitForCompletion(true);

			job = new Job(conf, "Update2");
			job.setJarByClass(GradientDescentLinReg.class);
			job.setMapperClass(UpdateMapper2.class);
			job.setReducerClass(UpdateReducer2.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("step_0_" + Integer.toString(i)));
			FileOutputFormat.setOutputPath(job, new Path("step_1_" + Integer.toString(i)));
			job.waitForCompletion(true);

			job = new Job(conf, "Update3");
			job.setJarByClass(GradientDescentLinReg.class);
			job.setMapperClass(UpdateMapper3.class);
			job.setReducerClass(UpdateReducer3.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("step_1_" + Integer.toString(i)));
			FileInputFormat.addInputPath(job, new Path(input + "_data"));
			FileOutputFormat.setOutputPath(job, new Path("step_2_" + Integer.toString(i)));
			job.waitForCompletion(true);

			job = new Job(conf, "Update4");
			job.setJarByClass(GradientDescentLinReg.class);
			job.setMapperClass(UpdateMapper4.class);
			job.setReducerClass(UpdateReducer4.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("step_2_" + Integer.toString(i)));
			FileOutputFormat.setOutputPath(job, new Path("step_3_" + Integer.toString(i)));
			job.waitForCompletion(true);

			job = new Job(conf, "Update5");
			job.setJarByClass(GradientDescentLinReg.class);
			job.setMapperClass(UpdateMapper5.class);
			job.setReducerClass(UpdateReducer5.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("step_3_" + Integer.toString(i)));
			FileInputFormat.addInputPath(job, new Path(input + "_alpha_" + Integer.toString(i)));
			FileOutputFormat.setOutputPath(job, new Path("step_4_" + Integer.toString(i)));
			job.waitForCompletion(true);

			job = new Job(conf, "Update6");
			job.setJarByClass(GradientDescentLinReg.class);
			job.setMapperClass(UpdateMapper6.class);
			job.setReducerClass(UpdateReducer6.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("step_4_" + Integer.toString(i)));

			if (i < iterations - 1)
			{
				FileOutputFormat.setOutputPath(job, new Path(input + "_alpha_" + Integer.toString(i + 1)));
				job.waitForCompletion(true);
			}
			else
			{
				FileOutputFormat.setOutputPath(job, new Path(output));
				System.exit(job.waitForCompletion(true) ? 1 : 0);
			}
		}
	}
}

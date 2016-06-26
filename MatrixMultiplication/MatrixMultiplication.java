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

public class MatrixMultiplication
{
	public static class Mapper1 extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] tsv = line.split("\t");

			String[] info = tsv[0].split(":");
			String matrixID = info[0].substring(0, 1);
			String rowID = info[0].substring(1);
			int r = Integer.parseInt(info[1]);

			for (int i = 1; i < tsv.length; i++)
			{
				String colID = Integer.toString(i);
				for (int j = 1; j <= r; j++)
				{
					String outputKey = null;
					if (matrixID.equals("A"))
						outputKey = rowID + ":" + colID + ":" + Integer.toString(j);
					else
						outputKey = Integer.toString(j) + ":" + rowID + ":" + colID;

					String outputValue = tsv[i] + ":" + matrixID + ":" + rowID + ":" + colID;
					context.write(new Text(outputKey), new Text(outputValue));
				}
			}
		}
	}

	public static class Reducer1 extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String a = null;
			String b = null;
			String rowID = null;
			String colID = null;

			for (Text val : values)
			{
				String[] csv = val.toString().split(":");
				if (csv[1].equals("A"))
				{
					a = csv[0];
					rowID = csv[2];
				}
				else
				{
					b = csv[0];
					colID = csv[3];
				}
			}

			String outputKey = rowID + ":" + colID;
			double prod = Double.parseDouble(a) * Double.parseDouble(b);
			String outputValue = Double.toString(prod);
			context.write(new Text(outputKey), new Text(outputValue));  
		}
	}

	public static class Mapper2 extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] tsv = line.split("\t");
			context.write(new Text(tsv[0]), new Text(tsv[1]));	
		}
	}

	public static class Reducer2 extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Double sum = 0.0;
			for (Text val : values)
				sum += Double.parseDouble(val.toString());
			context.write(key, new Text(Double.toString(sum)));
		}
	}

	public static class Mapper3 extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] tsv = line.split("\t");
			String[] csv = tsv[0].split(":");
			String rowID = csv[0];
			String colID = csv[1];

			context.write(new Text(rowID), new Text(tsv[1] + ":" + colID));
		}
	}

	public static class Reducer3 extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			List<String> list = new ArrayList<String>();
			for (Text val : values)
				list.add(val.toString());

			KeyComparator kc = new KeyComparator();
			Collections.sort(list, kc);

			StringBuilder val = new StringBuilder();
			for (int i = 0; i < list.size(); i++)
			{
				String[] csv = list.get(i).split(":");
				val.append(csv[0] + "\t");
			}
			val.deleteCharAt(val.length() - 1);
		
			context.write(new Text("C" + key.toString()), new Text(val.toString()));
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
			System.err.println("Usage: MatrixMultiplication <in> <out>");
			System.exit(2);
		}

		String input = otherArgs[0];
		String output = otherArgs[1];

		Job job = new Job(conf, "MatrixMultiplication");
		job.setJarByClass(MatrixMultiplication.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path("step1"));
		job.waitForCompletion(true);

		job = new Job(conf, "MatrixMultiplication");
		job.setJarByClass(MatrixMultiplication.class);
		job.setMapperClass(Mapper2.class);
		job.setReducerClass(Reducer2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("step1"));
		FileOutputFormat.setOutputPath(job, new Path("step2"));
		job.waitForCompletion(true);

		job = new Job(conf, "MatrixMultiplication");
		job.setJarByClass(MatrixMultiplication.class);
		job.setMapperClass(Mapper3.class);
		job.setReducerClass(Reducer3.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("step2"));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
	}
}

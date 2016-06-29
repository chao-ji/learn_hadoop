import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatrixTranspose
{
	public static class Mapper1 extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] tsv = line.split("\t");

			String matrixID = tsv[0].substring(0, 1);
			String rowID = tsv[0].substring(1);

			for (int i = 1; i < tsv.length; i++)
			{
				String colID = Integer.toString(i);
				String outputKey = matrixID + colID;
				String outputVal = tsv[i] + ":" + rowID;
				context.write(new Text(outputKey), new Text(outputVal));
			}
		}
	}

	public static class Reducer1 extends Reducer<Text, Text, Text, Text>
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
				if (i < list.size() - 1)
					val.append(csv[0] + "\t");
				else
					val.append(csv[0]);
			}

			context.write(key, new Text(val.toString()));
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
			System.err.println("Usage: MatrixTranspose <in> <out>");
			System.exit(2);
		}

		String input = otherArgs[0];
		String output = otherArgs[1];

		Job job = new Job(conf, "MatrixTranspose");
		job.setJarByClass(MatrixTranspose.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
	}
}

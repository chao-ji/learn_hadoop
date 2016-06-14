import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TableJoin
{
	public static class Map extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			int i = 0;

			String[] cols = line.split("\t");
			if (cols[0].equals("a"))
				context.write(new Text(cols[2]), new Text("a+" + cols[1]));
			else
				context.write(new Text(cols[1]), new Text("b+" + cols[2]));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			List<String> lista = new ArrayList<String>();
			List<String> listb = new ArrayList<String>();

			for (Text val : values)
			{
				String record = val.toString();
				if (record.charAt(0) == 'a')
					lista.add(record.substring(2));
				else if (record.charAt(0) == 'b')
					listb.add(record.substring(2));
			}

			if (lista.size() != 0 && listb.size() != 0)
				for (int i = 0; i < lista.size(); i++)
					for (int j = 0; j < listb.size(); j++)
						context.write(new Text(lista.get(i)), new Text(listb.get(j)));	
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage：TableJoin <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "multiple table join");
		job.setJarByClass(TableJoin.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

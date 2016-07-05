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

public class MatrixAddition
{
	public static class MAMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] tsv = line.split("\t");

			String[] csv = tsv[0].split(":");
			String matrixID = csv[0];
			String rowID = csv[1];

			for (int i = 1; i < tsv.length; i++)
			{
				String colID = Integer.toString(i);
				String outputKey = rowID;
				String outputVal = tsv[i] + ":" + matrixID + ":" + colID;
				context.write(new Text(outputKey), new Text(outputVal));
			}
		}
	}

	public static class MAReducer extends Reducer<Text, Text, Text, Text>
	{
		private static final String matrixA = "A";
		private static final String matrixB = "B";
		private String newMatrixID;
		private boolean sign;

		public void setup(Context context) throws IOException, InterruptedException
		{
			newMatrixID = context.getConfiguration().get("newMatrixID");
			sign = context.getConfiguration().get("sign").equals("true") ? true : false;
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			List<String> list = new ArrayList<String>();
			for (Text val : values)
				list.add(val.toString());

			KeyComparator kc = new KeyComparator();
			Collections.sort(list, kc);

			StringBuilder row = new StringBuilder();
			for (int i = 0; i < list.size(); i += 2)
			{
				String[] csv1 = list.get(i).split(":");
				String[] csv2 = list.get(i + 1).split(":");
				double val = 0.0;
				double val1 = Double.parseDouble(csv1[0]);
				double val2 = Double.parseDouble(csv2[0]);
				
				
				if (csv1[1].equals(matrixA) && csv2[1].equals(matrixB))
					val = sign ? val1 + val2 : val1 - val2;
				else if (csv2[1].equals(matrixA) && csv1[1].equals(matrixB))
					val = sign ? val2 + val1 : val2 - val1;
				
				if (i < list.size() - 1)
					row.append(Double.toString(val) + "\t");
				else
					row.append(Double.toString(val));
			}
			
			String outputKey = "C:" + key.toString();
			context.write(new Text(outputKey), new Text(row.toString()));
			
		}
	}

	static class KeyComparator implements Comparator<String>
	{
		public int compare(String s1, String s2)
		{
			String[] parts1 = s1.split(":");
			String[] parts2 = s2.split(":");
			return Integer.parseInt(parts1[parts1.length - 1]) - Integer.parseInt(parts2[parts2.length - 1]);
        }
    }

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4)
		{
			System.err.println("Usage: MatrixAddition <newMatrixID> <sign> <in> <out>");
		}

		conf.set("newMatrixID", otherArgs[0]);
		conf.set("sign", otherArgs[1].toLowerCase().equals("true") ? "true" : "false");

		String input = otherArgs[2];
		String output = otherArgs[3];

		Job job = Job.getInstance(conf, "MatrixScalarMultiplication");
		job.setJarByClass(MatrixAddition.class);
		job.setMapperClass(MAMapper.class);
		job.setReducerClass(MAReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
//		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);		
	}
}

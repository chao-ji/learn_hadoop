import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class LongestSubStrPal
{
	public static void main(String[] args) throws Exception
	{
		String input = args[0];
		String temp = "temp";
		String output = args[1];

		Job job1 = new Job();
		job1.setJarByClass(LongestSubStrPal.class);
		job1.setJobName("LongestSubStrPal");
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);

		FileInputFormat.addInputPath(job1, new Path(input));
		FileOutputFormat.setOutputPath(job1, new Path(temp));

		job1.waitForCompletion(true);

		
		Job job2 = new Job();
		job2.setJarByClass(LongestSubStrPal.class);
		job2.setJobName("LongestSubStrPal");
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);

		FileInputFormat.addInputPath(job2, new Path(temp));
		FileOutputFormat.setOutputPath(job2, new Path(output));

		job2.waitForCompletion(true);
	}
}

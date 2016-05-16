import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class BFSDriver
{
	public static void main(String[] args) throws Exception
	{
		String input = args[0];
		String output = args[1] + System.nanoTime();

		boolean done = false;

		while (!done)
		{
			Job job = new Job();
			job.setJarByClass(BFSDriver.class);
			job.setJobName("BFSDriver");
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(BFSMapper.class);
			job.setReducerClass(BFSReducer.class);

			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));		
			job.waitForCompletion(true);
			
			input = output + "/part-r-00000";
			done = true;
			Path path = new Path(input);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			Map<String, String> map = new HashMap<String, String>();
			String line = br.readLine();

			while (line != null)
			{
				String[] part = line.split(" |\t");
				String node = part[0];
				String dist = part[1];
				map.put(node, dist);
				line = br.readLine();
			}
			br.close();

			Iterator<String> it = map.keySet().iterator();
			while (it.hasNext())
			{
				String key = it.next();
				String val = map.get(key);
				if (val.equals("I"))
					done = false;
			}
			input = output;
			output = args[1] + System.nanoTime();
		}
	}
}

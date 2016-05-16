import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BFSReducer extends Reducer<LongWritable, Text, LongWritable, Text>
{
	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		String nodes = null;
		long min = Long.MAX_VALUE;
		for (Text value : values)
		{
			String[] part = value.toString().split(" ");

			if (part[0].equalsIgnoreCase("NODE"))
				nodes = part[1];
			else
			{
				long dist = Long.MAX_VALUE;
				if (!part[1].equals("I"))
					dist = Long.parseLong(part[1]);
				min = Math.min(min, dist);
			}
		}

		String val = min == Long.MAX_VALUE ? "I" : Long.toString(min);
		context.write(key, new Text(val + " " + nodes));
	}
}

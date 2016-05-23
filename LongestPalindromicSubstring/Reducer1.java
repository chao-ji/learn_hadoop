import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<IntWritable, Text, IntWritable, Text>
{
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		Text value = new Text();
		for (Text val : values)
			value = val;
		context.write(key, value);
	}
}

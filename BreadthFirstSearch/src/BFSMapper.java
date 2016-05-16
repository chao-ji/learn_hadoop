import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BFSMapper extends Mapper<LongWritable, Text, LongWritable, Text>
{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] part = line.split(" |\t");
		String dist = "I";
		if (!part[1].equals("I"))
			dist = Long.toString(Long.parseLong(part[1]) + 1);
		String[] node = part[2].split(":");

		for (int i = 0; i < node.length; i++)
			context.write(new LongWritable(Long.parseLong(node[i])), new Text("DIST " + dist));

		context.write(new LongWritable(Long.parseLong(part[0])), new Text("DIST " + part[1]));
		context.write(new LongWritable(Long.parseLong(part[0])), new Text("NODE " + part[2]));
	}
}

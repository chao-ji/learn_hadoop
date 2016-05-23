import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<LongWritable, Text, Text, Text>
{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] part = line.split(" |\t");
		int index = Integer.parseInt(part[0]);
		String s = part[1];
		
		int l = index;
		int h = index;
		while (l >= 0 && h <= s.length() - 1 && s.charAt(l) == s.charAt(h))
		{
			l--;
			h++;
		}

		context.write(new Text("key"), new Text(s.substring(l + 1, h)));
		if (index == s.length() - 1)
			return ;

		l = index;
		h = index + 1;
		while (l >= 0 && h <= s.length() - 1 && s.charAt(l) == s.charAt(h))
		{
			l--;
			h++;
		}
		context.write(new Text("key"), new Text(s.substring(l + 1, h)));
	}
}

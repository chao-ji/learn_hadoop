import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<Text, Text, Text, Text>
{
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		String pal = new String();
		for (Text value : values)
		{
			String s = value.toString();
			if (s.length() > pal.length())
				pal = s;
		}

		context.write(new Text("longest_substring_palindrome"), new Text(pal));
	}
}

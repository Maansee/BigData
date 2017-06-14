import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TextToNull {

	public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		public void map(LongWritable key, Text value, Context c)
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

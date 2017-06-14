import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CalcAvg 
{

	public static class MyMapperClass extends Mapper<LongWritable, Text, Text, FloatWritable>
	{
		public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException 
		{
			String[] str = value.toString().split("-");
			String city = str[2];
			Float sal = Float.parseFloat(str[4]);
			c.write(new Text(city), new FloatWritable(sal));
			
		}
	}
	
	
	public static class MyReducerClass extends Reducer<Text, FloatWritable, Text, FloatWritable>
	{
		
		public void reduce(Text key, Iterable<FloatWritable> values, Context c) throws IOException, InterruptedException
		{
			//Float avg = 0.0f;
			
			Float total = 0.0f;
			int count=0;
			
			for(FloatWritable val: values)
			{
				total+=val.get();
				count++;
			}
			
			float avg = total/count;
			
			
			c.write(key,new FloatWritable(avg));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
	Configuration config = new Configuration();
	Job j = Job.getInstance(config, "Calculate average of each city");
	j.setJarByClass(CalcAvg.class);
	j.setMapperClass(MyMapperClass.class);
	j.setReducerClass(MyReducerClass.class);
	j.setOutputKeyClass(Text.class);
	j.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(j, new Path(args[0]));
    FileOutputFormat.setOutputPath(j, new Path(args[1]));
	System.exit(j.waitForCompletion(true) ? 0 : 1);
	
	
	}

}

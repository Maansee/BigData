import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class CalcSal 
{
	public static class MyMapperClass extends Mapper<LongWritable, Text, Text, FloatWritable>
	{
		public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException 
		{
			String[] str = value.toString().split("-");
			//String city = str[2];
			Float sal = Float.parseFloat(str[4]);
			c.write(new Text("DummyKey"), new FloatWritable(sal));
		}
	}
	
	public static class MyReducerClass extends Reducer<Text, FloatWritable, NullWritable, FloatWritable>
	{
		public void reduce(Text key, Iterable<FloatWritable> values, Context c) throws IOException, InterruptedException
		{
		
			Float total = 0.0f;
			int count=0;
			
			for(FloatWritable val: values)
			{
				total+=val.get();
			}
			
			c.write(null,new FloatWritable(total));
			
			//how to add all individual cities together
		
		}

	}


public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
	 Configuration conf = new Configuration();
	    //conf.set("name", "value")
	    
	    Job job = Job.getInstance(conf, "Total salary");
	    job.setJarByClass(CalcSal.class);
	    job.setMapperClass(MyMapperClass.class);
	    
	    job.setReducerClass(MyReducerClass.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}

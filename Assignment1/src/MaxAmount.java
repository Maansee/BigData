import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MaxAmount {

	public static class MyMapper extends Mapper<LongWritable ,Text,Text,LongWritable>
	{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String info = value.toString();
			String[] part = info.split(";");
			String custID= part[1];
			Long TotCost = Long.parseLong(part[7]);
			
			context.write(new Text(custID),new LongWritable(TotCost));
 		}
		
		}
	
	public static class MyReducer extends Reducer<Text, LongWritable,LongWritable,Text>
	{
	
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException

	{
		int maxVal=0;
		for (LongWritable val:values){
			maxVal=(int) Math.max(maxVal, val.get());
		}
		context.write(new LongWritable(maxVal), new Text());
	}

	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		 Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    
		    Job job = Job.getInstance(conf, "Max Amt");
		    job.setJarByClass(MaxAmount.class);
		    job.setMapperClass(MyMapper.class);
		    //job.setCombinerClass(MyReducer.class);
		    job.setReducerClass(MyReducer.class);
		    //job.setNumReduceTasks(0);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		

	}

}

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class TopProduct {

	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException
		{
			String [] str = value.toString().split(";");
			String prodID = str[5];	
			long tot_sales = Long.parseLong(str[8]);
			c.write(new Text (prodID), new LongWritable(tot_sales));
		}
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		TreeMap<Long,String> TopProd = new TreeMap<Long, String>();
		
		
		public void reduce(Text key, Iterable<LongWritable> values, Context c)
		{
			String ProdID="";
			
			long sum = 0L;
			
			for(LongWritable val : values)
			{
				sum+=val.get();
			}
			
			ProdID = key.toString();
			TopProd.put(sum,ProdID);
		
		
		if (TopProd.size()>5)
		{
			TopProd.remove(TopProd.firstKey());
		}
		
		}
		
		public void cleanup(Context c) throws IOException, InterruptedException
		{
			for(Long var: TopProd.descendingMap().keySet())
			{
				c.write(new Text(TopProd.get(var)),new LongWritable(var));
			}
		}
		}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		 Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    
		    Job job = Job.getInstance(conf, "Top 4 Products");
		    job.setJarByClass(TopCustomer.class);
		    job.setMapperClass(MyMapper.class);
		    
		    //job.setPartitionerClass(MyPart.class);
		    job.setReducerClass(MyReducer.class);
		    //job.setNumReduceTasks(0);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

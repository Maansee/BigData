import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class PartEmp  {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key, Text value, Context context) {
			
	try {	
		
		String str[] = value.toString().split(",");
		String gender = str[3];
		String agesal= str[2]+"-"+str[4];
		
		
		
			context.write(new Text(gender), new Text(agesal));
		} 
	
		catch (IOException e) {
			
			e.printStackTrace();
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
		
	}
	}
	 public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
	   {
		 
		 private Text outputKey = new Text();
		 
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		 {
			 int max = 0;
			 for(Text val : values)
			 {
				 String [] str = val.toString().split("-");
				 if (Integer.parseInt(str[1])>max)
				 {
					 max = Integer.parseInt(str[1]);
					 String mykey =  key.toString()+','+ str[0];
					 outputKey.set(mykey);
				 }
				 
			 }
			 
			 context.write(outputKey, new IntWritable(max));
			 
		 }
	   }
	 
	 public static class PartitionNew extends Partitioner<Text, Text>
	 {

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) 
		{
			String str[] = value.toString().split(",");
			for(String x :str)
			{
				String each[]=x.split("-");
				int age = Integer.parseInt(each[0]);
				int sal = Integer.parseInt(each[1]);
			
			if(age<=20)
			{
				return 0;
			}
			
			else if (age>20 && age<=30)
			{
				return 1;
			}
			else
			{
				return 2;
			}	
		}
			return 0;
		
	 }
	 }

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		 Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    
		    Job job = Job.getInstance(conf, "Partioner Example");
		    job.setJarByClass(PartEmp.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setPartitionerClass(PartitionNew.class);
		    job.setNumReduceTasks(3);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}


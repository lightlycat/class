package edu.clu.cs.inclass.class4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinMaxCountDriver {
	 public static void main(String[] args) throws Exception {
	        
		    Job job = Job.getInstance(new Configuration(), "min max count");
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(MinMaxCountTuple.class);
		    
		    job.setJarByClass(MinMaxCountDriver.class);
		    job.setMapperClass(MinMaxCountMapper.class);
		    job.setReducerClass(MinMaxCountReducer.class);
		    job.setCombinerClass(MinMaxCountReducer.class);
		    
		    job.setNumReduceTasks(10);
		                
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		        
		    job.waitForCompletion(true);
		 }

}

package edu.clu.cs.inclass.class4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.clu.cs.inclass.class3.WordCountDriver;
import edu.clu.cs.inclass.class3.WordCountMapper;
import edu.clu.cs.inclass.class3.WordCountReducer;


public class InvertedIndexDriver {
	
	 public static void main(String[] args) throws Exception {
	        
		    Job job = Job.getInstance(new Configuration(), "inverted index");
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
		    job.setJarByClass(InvertedIndexDriver.class);
		    job.setMapperClass(InvertedIndexMapper.class);
		    job.setReducerClass(InvertedIndexReducer.class);
		    job.setCombinerClass(InvertedIndexReducer.class);
		    
		    job.setNumReduceTasks(10);
		                
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		        
		    job.waitForCompletion(true);
		 }
   
}

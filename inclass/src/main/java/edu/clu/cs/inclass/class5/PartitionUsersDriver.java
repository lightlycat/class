package edu.clu.cs.inclass.class5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartitionUsersDriver {
	public static void main(String[] args) throws Exception {
		
	    Job job = Job.getInstance(new Configuration(), "partitioing");
	    
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(PartitionUsersDriver.class);

		job.setMapperClass(LastAccessDateMapper.class);
		job.setReducerClass(ValueReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Set custom partitioner and min last access date
		job.setPartitionerClass(LastAccessDatePartitioner.class);
		LastAccessDatePartitioner.setMinLastAccessDate(job, 2008);
		job.setNumReduceTasks(7);

		//job.setOutputFormatClass(TextOutputFormat.class);
		//job.getConfiguration().set("mapred.textoutputformat.separator", "");

		System.exit(job.waitForCompletion(true) ? 0 : 1);


	}

	

}

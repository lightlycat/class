package edu.clu.cs.inclass.class5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopTenDriver {
	

	public static void main(String[] args) throws Exception {

	    Job job = Job.getInstance(new Configuration(), "top 10");
	    
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
		job.setJarByClass(TopTenDriver.class);

		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

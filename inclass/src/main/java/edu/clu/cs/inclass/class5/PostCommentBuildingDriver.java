package edu.clu.cs.inclass.class5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PostCommentBuildingDriver {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: PostCommentBuildingDriver <posts> <comments> <outdir>");
			System.exit(1);
		}

		Job job = Job.getInstance(conf, "PostCommentHierarchy");
		job.setJarByClass(PostCommentBuildingDriver.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),TextInputFormat.class, PostMapper.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class, CommentMapper.class);

		job.setReducerClass(PostCommentHierarchyReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}
}


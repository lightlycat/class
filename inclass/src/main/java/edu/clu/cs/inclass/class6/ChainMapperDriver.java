package edu.clu.cs.inclass.class6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ChainMapperDriver {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Chain Mapper");
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: ChainMapperReducer <tweets> <users> <out>");
			System.exit(2);
		}

		Path tweetInput = new Path(otherArgs[0]);
		Path userInput = new Path(otherArgs[1]);
		Path outputDir = new Path(otherArgs[2]);

		// Setup first job to counter user posts
		
		job.setJarByClass(ChainMapperDriver.class);
		
		ChainMapper.addMapper(job, UserIdCountMapper.class,LongWritable.class, 
				Text.class, Text.class, LongWritable.class, new Configuration(false));

		ChainMapper.addMapper(job, UserIdFollowerEnrichmentMapper.class,
				Text.class, LongWritable.class, Text.class, LongWritable.class,
				new Configuration(false));

		ChainReducer.setReducer(job, LongSumReducer.class, Text.class,
				LongWritable.class, Text.class, LongWritable.class,
				new Configuration(false));
				
		ChainReducer.addMapper(job, UserIdBinningMapper.class, Text.class,
				LongWritable.class, Text.class, LongWritable.class,
				new Configuration(false));

		job.setCombinerClass(LongSumReducer.class);

		FileInputFormat.addInputPath(job,tweetInput);

		// Configure multiple outputs
		FileOutputFormat.setOutputPath(job, outputDir);
		
		// Configure the MultipleOutputs by adding an output called "bins"
		// With the proper output format and mapper key/value pairs
		MultipleOutputs.addNamedOutput(job, UserIdBinningMapper.MULTIPLE_OUTPUTS_ABOVE_1000, 
				TextOutputFormat.class, Text.class, LongWritable.class);

		MultipleOutputs.addNamedOutput(job, UserIdBinningMapper.MULTIPLE_OUTPUTS_BELOW_1000, 
				TextOutputFormat.class, Text.class, LongWritable.class);
		
		// Enable the counters for the job
		// If there is a significant number of different named outputs, this
		// should be disabled
		MultipleOutputs.setCountersEnabled(job, true);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		// Add the user files to the DistributedCache
		FileStatus[] userFiles = FileSystem.get(conf).listStatus(userInput);
		for (FileStatus status : userFiles) {
			//job.addCacheFile(status.getPath().toUri());
			DistributedCache.addCacheFile(status.getPath().toUri(),job.getConfiguration());
		}
	
		job.waitForCompletion(true);
	}


}

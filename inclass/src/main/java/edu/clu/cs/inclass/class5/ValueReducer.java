package edu.clu.cs.inclass.class5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ValueReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

	protected void reduce(IntWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		for (Text t : values) {
			context.write(t, NullWritable.get());
		}
	}
}

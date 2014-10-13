package edu.clu.cs.inclass.class6;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//input is tweets

public class UserIdCountMapper extends Mapper<Object, Text, Text, LongWritable> {

	private static final LongWritable ONE = new LongWritable(1);
	private Text outkey = new Text();

	public void map(Object key, Text value,Context context)
					throws IOException, InterruptedException {

		String[] tokens = value.toString().split("\t");
		String userId = tokens[1];
		if (userId != null) {
			outkey.set(userId);
			context.write(outkey, ONE);
		}
	}
}


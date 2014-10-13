package edu.clu.cs.inclass.class6;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LongSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	private LongWritable outvalue = new LongWritable();

	public void reduce(Text key, Iterable<LongWritable> values, Context context)
					throws IOException, InterruptedException {

		int sum = 0;
		for (LongWritable value : values) {
			sum += value.get();
		}
		outvalue.set(sum);
		
		context.write(key, outvalue); 
	}
}

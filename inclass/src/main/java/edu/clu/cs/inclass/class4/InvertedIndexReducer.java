package edu.clu.cs.inclass.class4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer  extends Reducer<Text, Text, Text, Text> {
	private Text value = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

	}
}

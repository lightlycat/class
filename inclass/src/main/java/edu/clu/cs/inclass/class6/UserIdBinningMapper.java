package edu.clu.cs.inclass.class6;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class UserIdBinningMapper extends Mapper<Text, LongWritable, Text, LongWritable> {

	public static final String MULTIPLE_OUTPUTS_BELOW_1000 = "below1000";
	public static final String MULTIPLE_OUTPUTS_ABOVE_1000 = "above1000";
	
	private MultipleOutputs<Text, LongWritable> mos = null;

	public void setup(Context context) {
		mos = new MultipleOutputs<Text, LongWritable>(context);
	}

	public void map(Text key, LongWritable value,Context context)
			throws IOException, InterruptedException {
		if (Integer.parseInt(key.toString().split("\t")[1]) < 1000) {
			mos.write(MULTIPLE_OUTPUTS_BELOW_1000, key, value, MULTIPLE_OUTPUTS_BELOW_1000+"/part");
		} else {
			mos.write(MULTIPLE_OUTPUTS_ABOVE_1000, key, value, MULTIPLE_OUTPUTS_ABOVE_1000+"/part");
		}
	}

	public void cleanup(Context context) {
		try {
			mos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}


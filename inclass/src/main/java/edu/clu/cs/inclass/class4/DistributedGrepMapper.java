package edu.clu.cs.inclass.class4;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.clu.cs.inclass.utils.MRDPUtils;

public class DistributedGrepMapper extends Mapper<Object, Text, NullWritable, Text> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


	}
}
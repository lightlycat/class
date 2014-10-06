package edu.clu.cs.inclass.class5;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.clu.cs.inclass.utils.MRDPUtils;

public class LastAccessDateMapper extends Mapper<Object, Text, IntWritable, Text> {

	// This object will format the creation date string into a Date object
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	private IntWritable outkey = new IntWritable();

	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// Parse the input string into a nice map
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

		// Grab the last access date
		String strDate = parsed.get("LastAccessDate");

		// skip this record if date is null
		if (strDate != null) {
			try {
				// Parse the string into a Calendar object
				Calendar cal = Calendar.getInstance();
				cal.setTime(frmt.parse(strDate));
				outkey.set(cal.get(Calendar.YEAR));
				// Write out the year with the input value
				context.write(outkey, value);
			} catch (ParseException e) {
				// An error occurred parsing the creation Date string
				// skip this record
			}
		}
	}
}
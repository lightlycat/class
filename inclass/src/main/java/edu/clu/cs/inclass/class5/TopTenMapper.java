package edu.clu.cs.inclass.class5;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.clu.cs.inclass.utils.MRDPUtils;

public class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
	// Our output key and value Writables
	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Parse the input string into a nice map
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
		if (parsed == null) {
			return;
		}

		String userId = parsed.get("Id");
		String reputation = parsed.get("Reputation");

		// Get will return null if the key is not there
		if (userId == null || reputation == null) {
			// skip this record
			return;
		}

		repToRecordMap.put(Integer.parseInt(reputation), new Text(value));

		if (repToRecordMap.size() > 10) {
			repToRecordMap.remove(repToRecordMap.firstKey());
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Text t : repToRecordMap.values()) {
			context.write(NullWritable.get(), t);
		}
	}
}

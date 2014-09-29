package edu.clu.cs.inclass.class4;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import edu.clu.cs.inclass.utils.MRDPUtils;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

	private Text word = new Text();
	private Text docId = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// Parse the input string into a nice map
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

		// Grab the necessary XML attributes
		String txt = parsed.get("Body");
		String posttype = parsed.get("PostTypeId");
		String row_id = parsed.get("Id");

		// if the body is null, or the post is a question (1), skip
		if (txt == null || (posttype != null && posttype.equals("1"))) {
			return;
		}
		txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());

		
	}
}

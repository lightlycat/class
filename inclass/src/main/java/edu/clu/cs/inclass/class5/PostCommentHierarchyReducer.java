package edu.clu.cs.inclass.class5;

import java.io.IOException;
import java.util.ArrayList;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import edu.clu.cs.inclass.utils.MRDPUtils;

public class PostCommentHierarchyReducer  extends Reducer<Text, Text, Text, NullWritable> {

	private ArrayList<String> comments = new ArrayList<String>();
	private String post = null;

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// Reset variables
		post = null;
		comments.clear();

		// For each input value
		for (Text t : values) {
			// If this is the post record, store it, minus the flag
			if (t.charAt(0) == 'P') {
				post = t.toString().substring(1, t.toString().length()).trim();
			} else {
				// Else, it is a comment record. Add it to the list, minus
				// the flag
				comments.add(t.toString().substring(1, t.toString().length()).trim());
			}
		}

		// If post is not null
		if (post != null) {
			// nest the comments underneath the post element
			String postWithCommentChildren = MRDPUtils.nestElements(post, comments);
			
			if(postWithCommentChildren!=null)
			// write out the XML
			context.write(new Text(postWithCommentChildren),NullWritable.get());
		}
	}
	
}
package edu.clu.cs.inclass.class6;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class UserIdFollowerEnrichmentMapper extends Mapper<Text, LongWritable, Text, LongWritable> {

	private Text outKey = new Text();
	private HashMap<String, Integer> userIdToFollowers = new HashMap<String, Integer>();

	public void setup(Context context) {
		try {
			userIdToFollowers.clear();
			//URI[] files = context.getCacheFiles();

			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (files == null || files.length == 0) {
				throw new RuntimeException(
						"User information is not set in DistributedCache");
			}
			
			// Read all files in the DistributedCache
			for (Path p : files) {
				BufferedReader rdr = new BufferedReader(
						new InputStreamReader(
								new GZIPInputStream(new FileInputStream(
										new File(p.toString())))));

				String line;
				// For each record in the user file
				while ((line = rdr.readLine()) != null) {
					String[] tokens = line.split("\t");
					if (tokens[0] != null && tokens[3] != null) {
						String userId = tokens[0];
						int numFollowers = Integer.parseInt(tokens[3]);
						// Map the user ID to the number of followers
						userIdToFollowers.put(userId, numFollowers);
					}
				}
			}

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void map(Text key, LongWritable value,Context context)
					throws IOException, InterruptedException {
		
		Integer numFollowers = userIdToFollowers.get(key.toString());
		if (numFollowers != null) {
			outKey.set(key + "\t" + numFollowers);
			context.write(outKey,value);
		}
	}
}


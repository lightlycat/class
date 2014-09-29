package edu.clu.cs.inclass.class2;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSReadExample {
	
	public static void main(String[] args) throws Exception {
		
		FileSystem fs = FileSystem.get(new Configuration());
		InputStream in = null;
		try {
			in = fs.open(new Path(args[0]));
			IOUtils.copyBytes(in, System.out,4096);
		} finally {
			IOUtils.closeStream(in);
		}
	}
}

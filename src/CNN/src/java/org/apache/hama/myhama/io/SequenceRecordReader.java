package org.apache.hama.myhama.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SequenceRecordReader extends RecordReader<Text, Text> {
	
	private SequenceFile.Reader in;
	private long start;
	private long end;
	private boolean more = true;
	private Text key = null;
	private Text value = null;
	  
	@Override
	public void initialize(InputSplit split, Configuration conf) 
			throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) split;  
	    Path path = fileSplit.getPath();
	    FileSystem fs = path.getFileSystem(conf);
	    this.in = new SequenceFile.Reader(fs, path, conf);
	    this.end = fileSplit.getStart() + fileSplit.getLength();

	    if (fileSplit.getStart() > in.getPosition()) {
	    	in.sync(fileSplit.getStart()); // sync to start
	    }

	    this.start = in.getPosition();
	    more = start < end;
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!more) {
			return false;
	    }
	    
		long pos = in.getPosition();
	    key = (Text) in.next((Object) key);
	    if (key == null || (pos >= end && in.syncSeen())) {
	    	more = false;
	    	key = null;
	    	value = null;
	    } else {
	    	value = (Text) in.getCurrentValue((Object) value);
	    }
	    return more;
	}

	@Override
	public Text getCurrentKey() {
	    return key;
	}
	  
	@Override
	public Text getCurrentValue() {
		return value;
	}
	  
	/**
	 * Return the progress within the input split
	 * @return 0.0 to 1.0 of the input byte range
	 */
	public float getProgress() throws IOException {
		if (end == start) {
			return 0.0f;
	    } else {
	    	return Math.min(1.0f, (in.getPosition() - start) / (float)(end - start));
	    }
	}
	  
	public synchronized void close() throws IOException {
		in.close();
	}
}

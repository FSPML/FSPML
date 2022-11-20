package org.apache.hama.myhama.io;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hama.bsp.BSPJob;

public class SequenceBSPFileInputFormat extends BSPFileInputFormat<Text, Text> {
	
	@Override
	protected long getFormatMinSplitSize() {
		return SequenceFile.SYNC_INTERVAL;
	}
	
	@Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
            BSPJob job) throws IOException, InterruptedException {
        return new SequenceRecordReader();
    }

    @Override
    protected boolean isSplitable(BSPJob job, Path file) {
        return true;
    }
}
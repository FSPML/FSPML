/**
 * MyKVInputFormat.java
 */

package org.apache.hama.myhama.io;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.io.BSPFileInputFormat;
import org.apache.hama.myhama.io.RecordReader;
import org.apache.hama.myhama.io.TextRecordReader;


/**
 * MyKVInputFormat
 * 
 * @author
 * @version
 */
public class KeyValueInputFormat extends BSPFileInputFormat<Text, Text> {

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
            BSPJob job) throws IOException, InterruptedException {
        return new TextRecordReader();
    }

    @Override
    protected boolean isSplitable(BSPJob job, Path file) {
        return true;
    }
}
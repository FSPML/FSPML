/**
 * TextBSPFileOutputFormat.java
 */

package org.apache.hama.myhama.io;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * TextBSPFileOutputFormat
 * 
 * An example that extends BSPFileOutputFormat<Text,Text>.
 * 
 * @author
 * @version 
 */
public class TextBSPFileOutputFormat extends BSPFileOutputFormat<Text, Text> {
	
    @Override
    public RecordWriter<Text, Text> getRecordWriter(BSPJob job,
            TaskAttemptID taskId) throws IOException, InterruptedException {
        Path file = getOutputPath(job, taskId);
        FileSystem fs = file.getFileSystem(job.getConf());
        FSDataOutputStream fileOut = fs.create(file, false);
        return new TextRecordWriter(fileOut);
    }
    
    /**
     * Only used for checkpoint, test.
     */
    public RecordWriter<Text, Text> getRecordWriter(BSPJob job, Path file) 
    	throws IOException, InterruptedException {
        FileSystem fs = file.getFileSystem(job.getConf());
        FSDataOutputStream fileOut = fs.create(file, false);
        return new TextRecordWriter(fileOut);
    }
}

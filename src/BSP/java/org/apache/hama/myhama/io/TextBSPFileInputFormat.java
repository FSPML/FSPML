/**
 * TextBSPFileInputFormat.java
 */

package org.apache.hama.myhama.io;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hama.bsp.BSPJob;

/**
 * TextBSPFileInputFormat
 * 
 * An example that extends the BSPFileInputFormat<Text, Text>. This class can
 * support Key-Value pairs InputFormat and the source data are stored in TextFile.
 * 
 * @author
 * @version
 */
public class TextBSPFileInputFormat extends BSPFileInputFormat<Text, Text> {

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
            BSPJob job) throws IOException, InterruptedException {
        return new TextRecordReader();
    }

    @Override
    protected boolean isSplitable(BSPJob job, Path file) {
        CompressionCodec codec = new CompressionCodecFactory(job.getConf())
                .getCodec(file);
        return codec == null;
    }
}
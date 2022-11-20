/**
 * InputFormat.java
 */

package org.apache.hama.myhama.io;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hama.bsp.BSPJob;

/**
 * InputFormat
 * 
 * This is an abstract class. All user-defined InputFormat class must implement
 * two methods:getSplits() and createRecordReader();
 * 
 * @author
 * @version
 */
public abstract class InputFormat<K, V> {

    /**
     * This method is used for generating splits according to the input data.
     * The list of split will be used by JobInProgress, SimpleTaskScheduler and
     * Staff.
     * 
     * @param job
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract List<InputSplit> getSplits(BSPJob job) throws IOException,
            InterruptedException;

    /**
     * This method will return a user-defined RecordReader for reading data from
     * the original storage. It is used in Staff.
     * 
     * @param split
     * @param job
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract RecordReader<K, V> createRecordReader(InputSplit split,
            BSPJob job) throws IOException, InterruptedException;

    /**
     * This method is only used to read data from HBase. If the data is read
     * from the DFS you do not cover it. This method is primarily used to
     * initialize the HBase table and set Scan
     * 
     * @param configuration
     */
    public void initialize(Configuration configuration) {
    }

}

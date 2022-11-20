/**
 * OutPutFormat.java
 */

package org.apache.hama.myhama.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.TaskAttemptID;



/**
 * OutputFormat
 * 
 * This is an abstract class. All user-defined OutputFormat class must implement
 * the methods:getRecordWriter();
 * 
 * @author
 * @version
 */
public abstract class OutputFormat<K, V> {

    /**
     * Get the {@link RecordWriter} for the given task.
     * 
     * @param job
     *            the information about the current task.
     * @return a {@link RecordWriter} to write the output for the job.
     * @throws IOException
     */
    public abstract RecordWriter<K, V> getRecordWriter(BSPJob job,
            TaskAttemptID taskId) throws IOException, InterruptedException;

    /**
     * This method is only used to write data into HBase. If the data is wrote
     * into the DFS you do not cover it. This method is primarily used to
     * initialize the HBase table.
     * 
     * @param configuration
     */
    public void initialize(Configuration otherConf) {

    }
}

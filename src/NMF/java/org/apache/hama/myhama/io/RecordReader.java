/**
 * RecordReader.java
 */

package org.apache.hama.myhama.io;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * RecordReader
 * 
 * This class can read data in the form of Key-Value.
 * 
 * @author
 * @version
 */
public abstract class RecordReader<KEYIN, VALUEIN> implements Closeable {

    /**
     * Called once at initialization
     * 
     * @param genericSplit
     * @param conf
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract void initialize(InputSplit genericSplit, Configuration conf)
            throws IOException, InterruptedException;
    
    /**
     * Read the next key, value pair.
     * 
     * @return true if a key/value pair was read
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract boolean nextKeyValue() throws IOException,
            InterruptedException;

    /**
     * Get the current key
     * 
     * @return the current key or null if there is no current key
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract KEYIN getCurrentKey() throws IOException,
            InterruptedException;

    /**
     * Get the current value.
     * 
     * @return the object that was read
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract VALUEIN getCurrentValue() throws IOException,
            InterruptedException;

    /**
     * The current progress of the record reader through its data.
     * 
     * @return a number between 0.0 and 1.0 that is the fraction of the data
     *         read
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract float getProgress() throws IOException,
            InterruptedException;

    /**
     * Close the record reader
     */
    public abstract void close() throws IOException;
}

/**
 * BSPFileOutputFormat.java
 */

package org.apache.hama.myhama.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * BSPFileOutputFormat
 * 
 * This class is used for writing on the file system, such as HDFS.
 * 
 * @author
 * @version
 */
public abstract class BSPFileOutputFormat<K, V> extends OutputFormat<K, V> {

    private static final Log LOG = LogFactory.getLog(BSPFileOutputFormat.class);

    /**
     * If the output directory has existed, then delete it.
     */
    public static void checkOutputSpecs(BSPJob job, Path outputDir) {
        try {
            FileSystem fileSys = FileSystem.get(job.getConf());
            if (fileSys.exists(outputDir)) {
                fileSys.delete(outputDir, true);
            }
        } catch (IOException e) {
            // TODO This must be processed and tell the user.
            LOG.error(e.toString());
        }

    }

    /**
     * Set the {@link Path} of the output directory for the Termite job.
     * 
     * @param job
     *            The job configuration
     * @param outputDir
     *            the {@link Path} of the output directory for the Termite job.
     */
    public static void setOutputPath(BSPJob job, Path outputDir) {
        Configuration conf = job.getConf();
        checkOutputSpecs(job, outputDir);
        conf.set(Constants.USER_JOB_OUTPUT_DIR, outputDir.toString());
    }

    /**
     * Get the {@link Path} to the output directory for the Termite job.
     * 
     * @return the {@link Path} to the output directory for the Termite job.
     */
    public static Path getOutputPath(BSPJob job, TaskAttemptID taskId) {
        String name = job.getConf().get(Constants.USER_JOB_OUTPUT_DIR)
                + "/" + "task-" + taskId.toString().substring(26, 32);
        return name == null ? null : new Path(name);
    }
}

/**
 * BSPFileInputFormat.java
 */

package org.apache.hama.myhama.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJob;


/**
 * BSPFileInputFormat
 * 
 * This class is used for reading from the file system, such as HDFS.
 * 
 * @author
 * @version
 */
public abstract class BSPFileInputFormat<K, V> extends InputFormat<K, V> {
    private static final Log LOG = LogFactory.getLog(BSPFileInputFormat.class);
    private static final double SPLIT_SLOP = 1.1; // 10% slop

    private static final PathFilter hiddenFileFilter = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    /**
     * Proxy PathFilter that accepts a path only if all filters given in the
     * constructor do. Used by the listPaths() to apply the built-in
     * hiddenFileFilter together with a user provided one (if any).
     */
    private static class MultiPathFilter implements PathFilter {
        private List<PathFilter> filters;

        public MultiPathFilter(List<PathFilter> filters) {
            this.filters = filters;
        }

        public boolean accept(Path path) {
            for (PathFilter filter : filters) {
                if (!filter.accept(path)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Get the lower bound on split size imposed by the format.
     * 
     * @return the number of bytes of the minimal split for this format
     */
    protected long getFormatMinSplitSize() {
        return 1;
    }

    /**
     * Is the given filename splitable? Usually, true, but if the file is stream
     * compressed, it will not be.
     * 
     * @param BSPJob
     *            the job configuration
     * @param filename
     *            the file name to check
     * @return is this file splitable?
     */
    protected boolean isSplitable(BSPJob job, Path filename) {
        return true;
    }

    /**
     * Set a PathFilter to be applied to the input paths for the map-reduce job.
     * 
     * @param job
     *            the job to modify
     * @param filter
     *            the PathFilter class use for filtering the input paths.
     */
    public static void setInputPathFilter(Job job,
            Class<? extends PathFilter> filter) {
        job.getConfiguration().setClass("mapred.input.pathFilter.class",
                filter, PathFilter.class);
    }

    /**
     * Set the minimum input split size TODO This function is disable
     * 
     * @param job
     *            the job to modify
     * @param size
     *            the minimum size
     */
    public static void setMinInputSplitSize(Job job, long size) {
        job.getConfiguration().setLong("mapred.min.split.size", size);
    }

    /**
     * Get the minimum split size TODO This function is disable
     * 
     * @param job
     *            the job
     * @return the minimum number of bytes that can be in a split
     */
    public static long getMinSplitSize(Job job) {
        return job.getConfiguration().getLong("mapred.min.split.size", 1L);
    }

    /**
     * Set the maximum split size TODO This function is disable
     * 
     * @param job
     *            the job to modify
     * @param size
     *            the maximum split size
     */
    public static void setMaxInputSplitSize(Job job, long size) {
        job.getConfiguration().setLong("mapred.max.split.size", size);
    }

    /**
     * Get the maximum split size. TODO This function is disable
     * 
     * @param context
     *            the job to look at.
     * @return the maximum number of bytes a split can include
     */
    public static long getMaxSplitSize(Job context) {
        return context.getConfiguration().getLong("mapred.max.split.size",
                Long.MAX_VALUE);
    }

    /**
     * Get a PathFilter instance of the filter set for the input paths.
     * 
     * @return the PathFilter instance set for the job, NULL if none has been
     *         set.
     */
    public static PathFilter getInputPathFilter(JobContext context) {
        Configuration conf = context.getConfiguration();
        Class<?> filterClass = conf.getClass("mapred.input.pathFilter.class",
                null, PathFilter.class);
        return (filterClass != null) ? ( PathFilter ) ReflectionUtils
                .newInstance(filterClass, conf) : null;
    }

    /**
     * Generate the list of files and make them into FileSplits.
     */
    // changed at 2011-11-23
    @Override
    public List<InputSplit> getSplits(BSPJob job) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        for (FileStatus file : listStatus(job)) {
            Path path = file.getPath();
            FileSystem fs = path.getFileSystem(job.getConf());
            long length = file.getLen();
            BlockLocation[] blkLocations = 
            	fs.getFileBlockLocations(file, 0, length);
            if ((length != 0) && isSplitable(job, path)) {
               	int taskNum = job.getNumBspTask();
            	long splitSize = (long)Math.ceil(length/(double)taskNum);
            	long skewSplitSize = 0, migrateSize = 0;
            	boolean skewSplitDone = false;
            	double skew = Double.parseDouble(job.getDataLoadSkew());
            	
            	if (taskNum == 1) {
            		skewSplitDone = true;
            	} else {
            		migrateSize = (long)(Math.ceil((skew*splitSize)/(skew+taskNum)));
            		skewSplitSize = splitSize + (taskNum-1)*migrateSize;
            		splitSize = splitSize - migrateSize;
            	}
            	
            	LOG.info("[LARGE Split Size] " + skewSplitSize / (float)(1024*1024) + " MB");
                LOG.info("[OTHER Split Size] " + splitSize / (float)(1024*1024) + " MB");
                
                long bytesRemaining = length;
                while ((( double ) bytesRemaining) / splitSize > SPLIT_SLOP) {
                    int blkIndex = getBlockIndex(blkLocations, length
                            - bytesRemaining);
                    if (!skewSplitDone) {
                    	splits.add(new FileSplit(path, length - bytesRemaining,
                                skewSplitSize, blkLocations[blkIndex].getHosts()));
                        bytesRemaining -= skewSplitSize;
                        skewSplitDone = true;
                    } else {
                    	splits.add(new FileSplit(path, length - bytesRemaining,
                                splitSize, blkLocations[blkIndex].getHosts()));
                        bytesRemaining -= splitSize;
                    }
                }
                if (bytesRemaining != 0) {
                    splits.add(new FileSplit(path, length - bytesRemaining,
                            bytesRemaining,
                            blkLocations[blkLocations.length - 1].getHosts()));
                }
            	
 /*               long splitSize = 
                	(long)Math.ceil(length/(double)job.getNumBspTask());
                LOG.info("[Split Size] " + splitSize / (float)(1024*1024) + " MB");
                
                long bytesRemaining = length;
                while ((( double ) bytesRemaining) / splitSize > SPLIT_SLOP) {
                    int blkIndex = getBlockIndex(blkLocations, length
                            - bytesRemaining);
                    splits.add(new FileSplit(path, length - bytesRemaining,
                            splitSize, blkLocations[blkIndex].getHosts()));
                    bytesRemaining -= splitSize;
                }
                if (bytesRemaining != 0) {
                    splits.add(new FileSplit(path, length - bytesRemaining,
                            bytesRemaining,
                            blkLocations[blkLocations.length - 1].getHosts()));
                }*/
            } else if (length != 0) {
            	LOG.info("isSplitable = false");
                splits.add(new FileSplit(path, 0, length, blkLocations[0]
                        .getHosts()));
            } else {
            	LOG.info("create empty split for a zero length file");
                splits.add(new FileSplit(path, 0, length, new String[0]));
            }
        }
        return splits;
    }
    
    public InputSplit getSplit(BSPJob job) throws IOException {
    	InputSplit split = null;
        for (FileStatus file : listCheckpointStatus(job)) {
            Path path = file.getPath();
            FileSystem fs = path.getFileSystem(job.getConf());
            long length = file.getLen();
            BlockLocation[] blkLocations = 
            	fs.getFileBlockLocations(file, 0, length);
            if (length != 0) {
            	split = new FileSplit(path, 0, length, 
            			blkLocations[0].getHosts());
            	break;
            } else {
            	LOG.error("find an empty file with length-zero:" + path.toString());
            }
        }
        return split;
    }

    /**
     * List input directories. Subclasses may override to, e.g., select only
     * files matching a regular expression.
     * 
     * @param job
     *            the job to list input paths for
     * @return array of FileStatus objects
     * @throws IOException
     *             if zero items.
     */
    // changed at 2011-11-23
    protected List<FileStatus> listStatus(BSPJob job) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        Path[] dirs = getInputPaths(job);
        if (dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }
        List<IOException> errors = new ArrayList<IOException>();

        // creates a MultiPathFilter with the hiddenFileFilter and the
        // user provided one (if any).
        List<PathFilter> filters = new ArrayList<PathFilter>();
        filters.add(hiddenFileFilter);
        PathFilter inputFilter = new MultiPathFilter(filters);

        for (int i = 0; i < dirs.length; ++i) {
            Path p = dirs[i];
            FileSystem fs = p.getFileSystem(job.getConf());
            FileStatus[] matches = fs.globStatus(p, inputFilter);
            if (matches == null) {
                errors.add(new IOException("Input path does not exist: " + p));
            } else if (matches.length == 0) {
                errors.add(new IOException("Input Pattern " + p
                        + " matches 0 files"));
            } else {
                for (FileStatus globStat : matches) {
                    if (globStat.isDir()) {
                        for (FileStatus stat : fs.listStatus(
                                globStat.getPath(), inputFilter)) {
                            result.add(stat);
                        }
                    } else {
                        result.add(globStat);
                    }
                }
            }
        }

        if (!errors.isEmpty()) {
            throw new InvalidInputException(errors);
        }
        LOG.info("[Total Input Files] " + result.size());
        return result;
    }
    
    /**
     * only used for testing checkpoint
     * @param job
     * @return
     * @throws IOException
     */
    protected List<FileStatus> listCheckpointStatus(BSPJob job) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        Path[] dirs = getCheckpointInputPaths(job);
        if (dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }
        List<IOException> errors = new ArrayList<IOException>();

        // creates a MultiPathFilter with the hiddenFileFilter and the
        // user provided one (if any).
        List<PathFilter> filters = new ArrayList<PathFilter>();
        filters.add(hiddenFileFilter);
        PathFilter inputFilter = new MultiPathFilter(filters);

        for (int i = 0; i < dirs.length; ++i) {
            Path p = dirs[i];
            FileSystem fs = p.getFileSystem(job.getConf());
            FileStatus[] matches = fs.globStatus(p, inputFilter);
            if (matches == null) {
                errors.add(new IOException("Input path does not exist: " + p));
            } else if (matches.length == 0) {
                errors.add(new IOException("Input Pattern " + p
                        + " matches 0 files"));
            } else {
                for (FileStatus globStat : matches) {
                    if (globStat.isDir()) {
                        for (FileStatus stat : fs.listStatus(
                                globStat.getPath(), inputFilter)) {
                            result.add(stat);
                        }
                    } else {
                        result.add(globStat);
                    }
                }
            }
        }

        if (!errors.isEmpty()) {
            throw new InvalidInputException(errors);
        }
        LOG.info("number of checkpoint input files: " + result.size());
        return result;
    }

    protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
        return Math.max(minSize, Math.min(maxSize, blockSize));
    }

    protected int getBlockIndex(BlockLocation[] blkLocations, long offset) {
        for (int i = 0; i < blkLocations.length; i++) {
            // is the offset inside this block?
            if ((blkLocations[i].getOffset() <= offset)
                    && (offset < blkLocations[i].getOffset()
                            + blkLocations[i].getLength())) {
                return i;
            }
        }
        BlockLocation last = blkLocations[blkLocations.length - 1];
        long fileLength = last.getOffset() + last.getLength() - 1;
        throw new IllegalArgumentException("Offset " + offset
                + " is outside of file (0.." + fileLength + ")");
    }

    /**
     * Add a {@link Path} to the list of inputs for the BC_BSP job.
     * 
     * @param job
     *            The {@link Job} to modify
     * @param path
     *            {@link Path} to be added to the list of inputs for the BC_BSP
     *            job.
     */
    // changed at 2011-11-23
    public static void addInputPath(BSPJob job, Path path) throws IOException {
        Configuration conf = job.getConf();
        FileSystem fs = FileSystem.get(conf);
        path = path.makeQualified(fs);
        String dirStr = StringUtils.escapeString(path.toString());
        String dirs = conf.get(Constants.USER_JOB_INPUT_DIR);
        conf.set(Constants.USER_JOB_INPUT_DIR, dirs == null ? dirStr
                : dirs + "," + dirStr);
    }
    
    /**
     * only used for testing checkpoint
     * @param job
     * @param path
     * @throws IOException
     */
    public void addCheckpointInputPath(BSPJob job, Path path) throws IOException {
        Configuration conf = job.getConf();
        FileSystem fs = FileSystem.get(conf);
        path = path.makeQualified(fs);
        String dirStr = StringUtils.escapeString(path.toString());
        String dirs = conf.get(Constants.CheckPoint.CK_TASK_INPUT);
        conf.set(Constants.CheckPoint.CK_TASK_INPUT, dirs == null ? dirStr
                : dirs + "," + dirStr);
    }

    /**
     * Get the list of input {@link Path}s for the bsp job.
     * 
     * @param job
     *            The job configuration
     * @return the list of input {@link Path}s for the bsp job.
     */
    // changed at 2011-11-23
    public static Path[] getInputPaths(BSPJob job) {
        String dirs = job.getConf()
                .get(Constants.USER_JOB_INPUT_DIR, "");
        String[] list = StringUtils.split(dirs);
        Path[] result = new Path[list.length];
        for (int i = 0; i < list.length; i++) {
            result[i] = new Path(StringUtils.unEscapeString(list[i]));
        }
        return result;
    }
    
    /**
     * only used for testing checkpoint
     * @param job
     * @return
     */
    public Path[] getCheckpointInputPaths(BSPJob job) {
        String dirs = job.getConf()
                .get(Constants.CheckPoint.CK_TASK_INPUT, "");
        String[] list = StringUtils.split(dirs);
        Path[] result = new Path[list.length];
        for (int i = 0; i < list.length; i++) {
            result[i] = new Path(StringUtils.unEscapeString(list[i]));
        }
        return result;
    }

    @Override
    public abstract RecordReader<K, V> createRecordReader(InputSplit split,
            BSPJob job) throws IOException, InterruptedException;
}

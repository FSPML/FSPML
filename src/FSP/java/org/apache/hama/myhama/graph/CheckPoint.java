package org.apache.hama.myhama.graph;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.myhama.io.KeyValueInputFormat;
import org.apache.hama.myhama.io.RecordReader;
import org.apache.hama.myhama.io.RecordWriter;
import org.apache.hama.myhama.io.TextBSPFileOutputFormat;

/**
 * A simple checkpointing-based fault-tolerance method. 
 * Data are archived periodically and then any failure 
 * can be recovered from the most recent available checkpoint.
 * 
 * @author zhigang
 */
public class CheckPoint {
	private BSPJob jobConf;
	private String ckTaskDir;
	private int availCKVersion = -1; //available checkpoint version
	
	private RecordWriter<Text, Text> output;
	private RecordReader<Text,Text> input;
	
	/**
	 * Construct CheckPoint class.
	 * @param _jobConf
	 * @param _taskId
	 * @param _ckJobDir
	 */
	public CheckPoint(BSPJob _jobConf, TaskAttemptID _taskId, String _ckJobDir) {
		this.jobConf = _jobConf;
		this.ckTaskDir = _ckJobDir + "/task-" + _taskId.getIntegerId();
	}
	
	/**
	 * Prepare to archive a new checkpoint
	 * @param _newVersion
	 * @throws Exception
	 */
	public void befArchive(int _newVersion) throws Exception {
		TextBSPFileOutputFormat outputFormat = new TextBSPFileOutputFormat();
		outputFormat.initialize(this.jobConf.getConf());
		this.output = outputFormat.getRecordWriter(this.jobConf, getPath(_newVersion));
	}
	
	/**
	 * Archive a vertex_id and vertex_value.
	 * @param key
	 * @param value
	 * @throws Exception
	 */
	public void archive(String key, String value) throws Exception {
		output.write(new Text(key), new Text(value));
	}
	
	/**
	 * Cleanup after archiving checkpoint.
	 * @param _newVersion
	 * @throws Exception
	 */
	public void aftArchive(int _newVersion) throws Exception {
		this.output.close(this.jobConf);
		
		/** delete old checkpoint to save storage space */
		Path delPath = getPath(-1);
		if (delPath != null) {
			FileSystem fs = delPath.getFileSystem(this.jobConf.getConf());
			fs.delete(delPath, true);
			fs.close();
		}
		
		this.availCKVersion = _newVersion;
	}
	
	/**
	 * Get {@link RecordReader} of an existing checkpoint file.
	 * @return NULL if no checkpoint is available
	 * @throws Exception
	 */
	public RecordReader<Text, Text> befLoad() throws Exception {
		if (this.availCKVersion < 0) {
			return null;
		}
		
		KeyValueInputFormat inputFormat = new KeyValueInputFormat();
		inputFormat.initialize(this.jobConf.getConf());
		inputFormat.addCheckpointInputPath(this.jobConf, new Path(this.ckTaskDir));
		org.apache.hadoop.mapreduce.InputSplit split = inputFormat.getSplit(this.jobConf);
		if (split == null) {
			return null;
		}
		
		this.input = inputFormat.createRecordReader(split, this.jobConf);
		this.input.initialize(split, this.jobConf.getConf());
		return this.input;
	}
	
	/**
	 * Cleanup after loading checkpoint.
	 * @throws Exception
	 */
	public void aftLoad() throws Exception {
		if (this.input != null) {
			this.input.close();
			this.input = null;
		}
	}
	
	/**
	 * Get the outputpath for checkpoint file.
	 * If newVersion > 0, return a path for new checkpoint-file, 
	 * else, return the path of available-checkpoint-file.
	 * @param newVersion
	 * @return
	 */
	private Path getPath(int newVersion) {
		if (newVersion > 0) {
			return new Path(this.ckTaskDir + "/ck-" + newVersion);
		} else if (this.availCKVersion == -1) {
			return null;
		} else {
			return new Path(this.ckTaskDir + "/ck-" + this.availCKVersion);
		}
	}
}

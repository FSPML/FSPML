package org.apache.hama.monitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.myhama.api.GraphRecord;

public class TaskInformation implements Writable {
	private int taskId;
	private int verMinId, verMaxId;
	private int port;
	private String hostName;
	
	private int verNum = 0;
	
	private JobInformation jobInfo;
	
	private GraphRecord[] initCentroids; //only available for the first task, i.e., task-0
	
	public TaskInformation () {
		
	}
	
	public TaskInformation (int _taskId, int _minVerId, int _port, String _hostName) {
		taskId = _taskId;
		verMinId = _minVerId;
		port = _port;
		hostName = _hostName;
	}
	
	public void init(JobInformation _jobInfo) {
		this.jobInfo = _jobInfo;
		this.verMaxId = jobInfo.getVerMaxId(taskId);
		this.verNum = verMaxId - verMinId + 1;
	}
	
	public int getTaskId() {
		return taskId;
	}
	
	public int getVerMinId() {
		return verMinId;
	}
	
	public int getVerMaxId() {
		return verMaxId;
	}
	
	public int getPort() {
		return port;
	}
	
	public String getHostName() {
		return hostName;
	}
	
	public void setVerNum(int _verNum) {
		this.verNum = _verNum;
	}
	
	public int getVerNum() {
		return this.verNum;
	}
	
	public void setInitCentroids(GraphRecord[] init) {
		this.initCentroids = init;
	}
	
	public GraphRecord[] getInitCentroids() {
		return this.initCentroids;
	}
	
	public int getNumOfInitCentroids() {
		return (this.initCentroids==null? 0:this.initCentroids.length);
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[taskId]="); sb.append(this.taskId);
		sb.append(" #vertex="); sb.append(this.verNum);
		
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		taskId = in.readInt();
		verMinId = in.readInt();
		port = in.readInt();
		hostName = Text.readString(in);
		
		this.verNum = in.readInt();
		
		int numOfInitCentroids = in.readInt();
		this.initCentroids = new GraphRecord[numOfInitCentroids];
		for (int i = 0; i < numOfInitCentroids; i++) {
			this.initCentroids[i] = new GraphRecord();
			this.initCentroids[i].readFields(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(taskId);
		out.writeInt(verMinId);
		out.writeInt(port);
		Text.writeString(out, hostName);
		
		out.writeInt(this.verNum);
		
		int numOfInitCentroids = this.getNumOfInitCentroids();
		out.writeInt(numOfInitCentroids);
		for (int i = 0; i < numOfInitCentroids; i++) {
			this.initCentroids[i].write(out);
		}
	}

}

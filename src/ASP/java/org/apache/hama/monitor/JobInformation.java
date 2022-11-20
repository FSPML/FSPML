package org.apache.hama.monitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.graph.CheckPointStatus;

public class JobInformation implements Writable {
	private static final Log LOG = LogFactory.getLog(JobInformation.class);
	private BSPJob job;
	private int taskNum = 0;
	private int verNum = 0;
	private long edgeNum = 0L;
	private int verMinId = Integer.MAX_VALUE, verMaxId = 0;
	private int[] verMinIds, verMaxIds;
	private int[] taskIds, ports;
	private String[] hostNames;
	
	private String ckDir;
	private ArrayList<CheckPointStatus> ckArcHistInfo;
	private ArrayList<CheckPointStatus> ckLoadHistInfo;
	
	//=====================================
	// Only available in JobInProgress
	//=====================================
	
	private ArrayList<Double> iteTime;
	private ArrayList<String> iteCommand;
	private ArrayList<Integer> intervals;
		
	public JobInformation() {
		
	}
	
	public JobInformation (BSPJob _job, int _taskNum) {
		this.job = _job;
		this.taskNum = _taskNum;
		this.taskIds = new int[_taskNum];
		this.verMinIds = new int[_taskNum];
		this.verMaxIds = new int[_taskNum];
		this.ports = new int[_taskNum];
		this.hostNames = new String[_taskNum];
		
		//local variables.
		iteTime = new ArrayList<Double>();
		iteCommand = new ArrayList<String>(); 
		intervals = new ArrayList<Integer>(); 
		
		//checkpoint
		this.ckArcHistInfo = new ArrayList<CheckPointStatus>();
		this.ckLoadHistInfo = new ArrayList<CheckPointStatus>();
	}
	
	/**
	 * The first phase for initializing some global variables.
	 * Only invoked by {@link JobInProgress} at Master.
	 * @param taskId
	 * @param tInfo
	 */
	public synchronized void buildInfo(int taskId, TaskInformation tInfo) {
		this.taskIds[taskId] = tInfo.getTaskId();
		this.verMinIds[taskId] = tInfo.getVerMinId();
		this.ports[taskId] = tInfo.getPort();
		this.hostNames[taskId] = tInfo.getHostName();
	}
	
	public String getCheckPointDirAftBuildRouteTable() {
		return this.ckDir;
	}
	
	/**
	 * Only invoked by {@link JobInProgress} at the Master.
	 * @param _verNum
	 */
	public void initAftBuildingInfo(int _verNum, String _ckDir) {
		this.ckDir = _ckDir;
		initVerIdAndNums(_verNum);
	}
	
	/**
	 * The second phase for initializing remaining global variables 
	 * after graph data have been loaded onto local disks/memory.
	 * Only invoked by {@link JobInProgress} at Master.
	 * @param taskId
	 * @param tInfo
	 */
	public synchronized void registerInfo(int taskId, TaskInformation tInfo) {

	}
	
	private void initVerIdAndNums(int _verNum) {
		int[] tmpMin = new int[taskNum], tmpId = new int[taskNum];
		for (int i = 0; i < taskNum; i++) {
			tmpMin[i] = verMinIds[i];
			tmpId[i] = taskIds[i];
		}

		for (int i = 0, swap = 0; i < taskNum; i++) {
			for (int j = i + 1; j < taskNum; j++) {
				if (tmpMin[i] > tmpMin[j]) {
					swap = tmpMin[j]; tmpMin[j] = tmpMin[i]; tmpMin[i] = swap;
					swap = tmpId[j]; tmpId[j] = tmpId[i]; tmpId[i] = swap;
				}
			}
		}

		for (int i = 1; i < taskNum; i++) {
			verMaxIds[tmpId[i-1]] = tmpMin[i] - 1;
		}
		verMaxIds[tmpId[taskNum-1]] = _verNum + verMinIds[tmpId[0]] - 1;
		this.verMinId = tmpMin[0];
		this.verMaxId = verMaxIds[tmpId[taskNum-1]];
		this.verNum = _verNum;
	}
	
	public int getVerNum() {
		return this.verNum;
	}
	
	public long getEdgeNum() {
		return this.edgeNum;
	}
	
	public int getVerMinId() {
		return this.verMinId;
	}
	
	public int getVerMaxId() {
		return this.verMaxId;
	}
	
	public int[] getTaskIds() {
		return taskIds;
	}
	
	public int[] getVerMinIds() {
		return verMinIds;
	}
	
	public int[] getVerMaxIds() {
		return verMaxIds;
	}
	
	public int getVerMaxId(int _taskId) {
		return verMaxIds[_taskId];
	}
	
	public int[] getPorts() {
		return ports;
	}
	
	public String[] getHostNames() {
		return hostNames;
	}
	
	public void recordIterationCommand(String command) {
		this.iteCommand.add(command);
	}
	
	public void recordIterationRuntime(double time) {
		this.iteTime.add(time);
	}
	
	public void recordIterationInterval(int interval) {
		this.intervals.add(interval);
	}
	
	public double computeRecoveryTime(int start) {
		double sum = 0.0;
		if (start>0 && start<this.iteTime.size()) {
			if (this.getLastCheckPointLoadLocation()
					> this.getLastCheckPointArcLocation()) {
				//avoid computing time repeatly if recovery has been performed
				start = this.getLastCheckPointLoadLocation();
				sum -= getLastCheckPointLoadTime();
			} else {
				sum -= getLastCheckPointArcTime();
			}
			
			if (start < this.iteTime.size()) {
				for (int idx = start; idx < this.iteTime.size(); idx++) {
					sum += this.iteTime.get(idx);
				}
			}
		}
		return sum;
	}
	
	public void recordCheckPointInfo(CheckPointStatus cks) {
		switch (cks.getType()) {
		case Archive: this.ckArcHistInfo.add(cks); break;
		case Load: this.ckLoadHistInfo.add(cks); break;
		case Uninitialized: 
		default:
			LOG.error("record an uninitialized CheckPointStatus");
		}	
	}
	
	public double getAvgCheckPointArcVertTime() {
		double avg = 0.0;
		if (this.ckArcHistInfo.size() > 0) {
			double sum = 0.0;
			for (CheckPointStatus cks: this.ckArcHistInfo) {
				sum += cks.getRuntime();
			}
			avg = sum / this.ckArcHistInfo.size();
		}
		return avg;
	}
	
	public int getLastCheckPointArcLocation() {
		int loc = 0, num = this.ckArcHistInfo.size();
		if (num > 0) {
			loc = this.ckArcHistInfo.get(num-1).getLoc();
		}
		return loc;
	}
	
	private int getLastCheckPointLoadLocation() {
		int loc = 0, num = this.ckLoadHistInfo.size();
		if (num > 0) {
			loc = this.ckLoadHistInfo.get(num-1).getLoc();
		}
		return loc;
	}
	
	private double getLastCheckPointArcTime() {
		double time = 0.0;
		int num = this.ckArcHistInfo.size();
		if (num > 0) {
			time = this.ckArcHistInfo.get(num-1).getRuntime();
		}
		return time;
	}
	
	private double getLastCheckPointLoadTime() {
		double time = 0.0;
		int loadLoc = this.getLastCheckPointLoadLocation();
		if (loadLoc>this.getLastCheckPointArcLocation()
				&& loadLoc<this.iteTime.size()) {
			int num = this.ckLoadHistInfo.size();
			time = this.ckLoadHistInfo.get(num-1).getRuntime();
		}
		return time;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("\n");
		
		sb.append("\nDetailInfo: iteCounter    command    time    interval(ms)");
		for (int index = 0; index < this.iteCommand.size(); index++) {
			sb.append("\n   ite[" + (index+1) + "]  ");
			sb.append(this.iteCommand.get(index)); sb.append("\t");
			sb.append(this.iteTime.get(index)); sb.append("\t");
			sb.append(this.intervals.get(index));
		}
		
		sb.append("\n\nCheckPoint Archiving Info: location  arcVertNum  runtime(seconds)");
		for (CheckPointStatus cks: this.ckArcHistInfo) {
			sb.append("\n   ite[" + cks.getLoc() + "]  ");
			sb.append(cks.getVertNum()); sb.append("\t");
			sb.append(cks.getRuntime()); sb.append("\t");
		}
		
		sb.append("\n\nCheckPoint Loading Info: location  arcVertNum  runtime(seconds)");
		for (CheckPointStatus cks: this.ckLoadHistInfo) {
			sb.append("\n   ite[" + cks.getLoc() + "]  ");
			sb.append(cks.getVertNum()); sb.append("\t");
			sb.append(cks.getRuntime()); sb.append("\t");
		}
		sb.append("\n");
		
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.verNum = in.readInt();
		this.edgeNum = in.readLong();
		this.verMinId = in.readInt();
		this.verMaxId = in.readInt();
		this.ckDir = Text.readString(in);
		
		this.taskNum = in.readInt();
		this.taskIds = new int[taskNum];
		this.verMinIds = new int[taskNum];
		this.verMaxIds = new int[taskNum];
		this.ports = new int[taskNum];
		this.hostNames = new String[taskNum];
		for (int i = 0; i < taskNum; i++) {
			taskIds[i] = in.readInt();
			verMinIds[i] = in.readInt();
			verMaxIds[i] = in.readInt();
			ports[i] = in.readInt();
			hostNames[i] = Text.readString(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.verNum);
		out.writeLong(this.edgeNum);
		out.writeInt(this.verMinId);
		out.writeInt(this.verMaxId);
		Text.writeString(out, this.ckDir);
		
		out.writeInt(this.taskNum);
		for (int i = 0; i < taskNum; i++) {
			out.writeInt(taskIds[i]);
			out.writeInt(verMinIds[i]);
			out.writeInt(verMaxIds[i]);
			out.writeInt(ports[i]);
			Text.writeString(out, hostNames[i]);
		}
	}
}

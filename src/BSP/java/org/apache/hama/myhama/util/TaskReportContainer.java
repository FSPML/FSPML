package org.apache.hama.myhama.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TaskReportContainer implements Writable {
	
	private double currentProgress = 0.0f;
	private double usedMemory = 0.0f;
	private double totalMemory = 0.0f;
	
	public TaskReportContainer() {
		
	}
	
	/**
	 * Construct a report
	 * @param currentProgress
	 * @param usedMemory
	 * @param totalMemory
	 */
	public TaskReportContainer(double currentProgress, double usedMemory, double totalMemory) {
		this.currentProgress = currentProgress;
		this.usedMemory = usedMemory;
		this.totalMemory = totalMemory;
	}
	
	public double getCurrentProgress() {
		return currentProgress;
	}

	public void setCurrentProgress(double currentProgress) {
		this.currentProgress = currentProgress;
	}

	public double getUsedMemory() {
		return usedMemory;
	}

	public void setUsedMemory(double usedMemory) {
		this.usedMemory = usedMemory;
	}

	public double getTotalMemory() {
		return totalMemory;
	}

	public void setTotalMemory(double totalMemory) {
		this.totalMemory = totalMemory;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.currentProgress = in.readDouble();
		this.usedMemory = in.readDouble();
		this.totalMemory = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(this.currentProgress);
		out.writeDouble(this.usedMemory);
		out.writeDouble(this.totalMemory);
	}
}

package org.apache.hama.myhama.graph;

import org.apache.hama.Constants.CheckPoint.CheckPointType;

/**
 * This class is used to record some statistics when 
 * performing checkpoint.
 * @author root
 *
 */
public class CheckPointStatus {
	/** type of checkpoint: Archive or Load */
	private CheckPointType type = CheckPointType.Uninitialized;
	/** data are archived/loaded at the end of the loc-th iteration */
	private int loc = -1;
	/** the number of archived/loaded vertices */
	private int num = -1;
	/** runtime of archiving/loading data (seconds) */
	private double time = 0;
	
	/**
	 * Record a checkpoint status.
	 * @param _type Archive or Load
	 * @param _loc  at which iteration
	 * @param _arcNum  number of archived/loaded vertices
	 * @param _time  runtime of archiving/loading data
	 */
	public CheckPointStatus(CheckPointType _type, int _loc, 
			int _num, double _time) {
		this.type = _type;
		this.loc = _loc;
		this.num = _num;
		this.time = _time;
	}
	
	public CheckPointType getType() {
		return this.type;
	}
	
	public int getLoc() {
		return this.loc;
	}
	
	public int getVertNum() {
		return this.num;
	}
	
	public double getRuntime() {
		return this.time;
	}
}

package org.apache.hama.myhama.util;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Adjust the synchronous barrier interval based on dynamically collected 
 * parameters.
 * @author root
 *
 */
public class IntervalAdjuster {
	private static final Log LOG = LogFactory.getLog(IntervalAdjuster.class);
	
	/** runtime of performing a traditional BSP iteration (including sync. cost) */
	private double timeOfFullIteration = 0.0;
	
	private int step = 2;
	private int delta = 2; //make sure that the performance is steady
	
	private double timeOfAllSync = 0.0;
	private int numOfAllSync = 0;
	private double timeOfAvgSync = 0.0;
	
	private double interval = 0.0;
	
	private double targetDeltaProg = 0.0;
	private double realDeltaProg = 0.0;
	private int realCounter = 0; 
	
	
	private boolean fixed = false;
	
	public IntervalAdjuster(double time) {
		this.timeOfFullIteration = time;
		//make sure that we can adjust interval when finishing the 1st iteration (isAdjust())
		
		this.fixed = false;
		
		LOG.info("step=" + this.step 
				+ ", delta=" + this.delta);
	}
	
	public int computeNewInterval(double olderDeltaProg, double oldDeltaProg, int iteNum, boolean isGMM) {

		return 250;

//		StringBuffer sb = new StringBuffer("computeNewInterval-" + iteNum);
//		sb.append("\nolder=" + olderDeltaProg);
//		sb.append("\nold=" + oldDeltaProg);
//		sb.append("\nreal=" + this.realDeltaProg);
//		sb.append("\ntarget=" + this.targetDeltaProg);
//		
//		if (this.fixed) {
//			return (int)Math.round(this.interval);
//		}
//		LOG.info("this is " + iteNum + " ci, realDeltaProg is " + this.realDeltaProg + " targetDeltaprog is " + this.targetDeltaProg);
//		if (iteNum == 1) {
//			sb.append("\nInitialized");
//			this.targetDeltaProg = 0.0;
//			this.interval = this.timeOfFullIteration / this.step;
//		} else if ((isGMM&&(this.realDeltaProg>=this.targetDeltaProg)) 
//				|| ((!isGMM)&&(this.realDeltaProg<=this.targetDeltaProg))) {
//			sb.append("\nFixed, large=" + (this.interval*this.step) 
//					+ ", small=" + this.interval
//					+ ", final=" + (this.interval*this.step + this.interval)/2);
//			//this.interval = this.interval * this.window;
//			this.interval = (this.interval*this.step + this.interval) / 2;
//			this.targetDeltaProg = 0.0;
//			this.fixed = true;
//		} else {
//			this.targetDeltaProg = oldDeltaProg;
//			sb.append("\nSPLIT");
//			this.interval = this.interval / this.step;
//
//		}
//		sb.append("\nnew_target=" + this.targetDeltaProg);
//		sb.append("\nnew_interval=" + (int)Math.round(this.interval));
//		LOG.info(sb.toString());
//		
//		this.realCounter = 0;
//		this.realDeltaProg = 0.0;
//		return (int)Math.round(this.interval);
	}
	
	/**
	 * Update statistics after performing one time-sync. iteration.
	 * @param syncTime Runtime of performing one time-sync. barrier
	 * @param deltaProg New progress in one iteration
	 */
	public void updateProg(double deltaProg) {
		this.timeOfAvgSync = this.timeOfAllSync / this.numOfAllSync;
		double extraSyncTime = (this.step - 1) * this.timeOfAvgSync;
		
		this.realDeltaProg = deltaProg * (this.step - extraSyncTime/this.interval);
	}
	
	public void updateSyncTime(double syncTime) {
		this.timeOfAllSync += syncTime;
		this.numOfAllSync++;
	}
	
}

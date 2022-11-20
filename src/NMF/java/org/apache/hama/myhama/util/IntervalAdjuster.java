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
	private int window = 4;
	private int delta = 2; //make sure that the performance is steady
	
	private double timeOfAllSync = 0.0;
	private int numOfAllSync = 0;
	private double timeOfAvgSync = 0.0;
	
	private double interval = 0.0;
	
	private double targetDeltaProg = 0.0;
	private double realDeltaProg = 0.0;
	private int realCounter = 0; 
	
	private ArrayList<Double> hist;
	
	private boolean fixed = false;
	
	public IntervalAdjuster(double time) {
		this.timeOfFullIteration = time;
		if (this.window < (this.step+this.delta)) {
			this.window = this.step + this.delta;
		}
		//make sure that we can adjust interval when finishing the 1st iteration (isAdjust())
		this.realCounter = this.window;
		
		this.fixed = false;
		this.hist = new ArrayList<Double>();
		
		LOG.info("step=" + this.step 
				+ ", window=" + this.window
				+ ", delta=" + this.delta);
	}
	
	public int computeNewInterval(double olderDeltaProg, double oldDeltaProg, int iteNum, boolean isGMM) {

		/*
		StringBuffer sb = new StringBuffer("computeNewInterval-" + iteNum);
		sb.append("\nhist=" + this.hist.toString());
		sb.append("\nolder=" + olderDeltaProg);
		sb.append("\nold=" + oldDeltaProg);
		sb.append("\nreal=" + this.realDeltaProg);
		sb.append("\ntarget=" + this.targetDeltaProg);
		
		if (this.fixed) {
			return (int) Math.round(this.interval);
		}
		
		if (iteNum == 1) {
			sb.append("\nInitialized");
			this.targetDeltaProg = 0.0;
			this.interval = this.timeOfFullIteration / this.step;
		} else if ((isGMM && (this.realDeltaProg >= this.targetDeltaProg)) 
				|| ((!isGMM) && (this.realDeltaProg <= this.targetDeltaProg))) {
			sb.append("\nFixed, large=" + (this.interval * this.step) 
					+ ", small=" + this.interval
					+ ", final=" + (this.interval * this.step + this.interval) / 2);
			//this.interval = this.interval * this.window;
			this.interval = (this.interval*this.step + this.interval) / 2;
			this.targetDeltaProg = 0.0;
			this.fixed = true;
		} else {
			this.timeOfAvgSync = this.timeOfAllSync / this.numOfAllSync;
			double extraSyncTime = (this.step - 1) * this.timeOfAvgSync;
			
			double diff = olderDeltaProg - oldDeltaProg;
			this.targetDeltaProg = (oldDeltaProg-diff) * (1 + extraSyncTime/this.interval);
			sb.append(",diff=" + diff);
			sb.append(",extra=" + extraSyncTime);
			
			sb.append("\nSPLIT");
			this.interval = this.interval / this.step;

		}
		sb.append("\nnew_target=" + this.targetDeltaProg);
		sb.append("\nnew_interval=" + (int) Math.round(this.interval));
		LOG.info(sb.toString());
		
		this.realCounter = 0;
		this.realDeltaProg = 0.0;
		this.hist.clear();
		return (int) Math.round(this.interval);
		*/
		return 595;
	}
	
	/**
	 * Update statistics after performing one time-sync. iteration.
	 * @param syncTime Runtime of performing one time-sync. barrier
	 * @param deltaProg New progress in one iteration
	 */
	public void updateProg(double deltaProg) {
		this.hist.add(deltaProg);
		this.realCounter++;
		if (this.realCounter > this.delta) {
			this.realDeltaProg += deltaProg;
		} //ignore the first "this.delta" iterations since the performance is not steady
	}
	
	public void updateSyncTime(double syncTime) {
		this.timeOfAllSync += syncTime;
		this.numOfAllSync++;
	}
	
	public boolean isAdjust() {
		return (this.realCounter>=this.window);
	}
}
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
	
	
	private double timeOfAllSync = 0.0;
	private int numOfAllSync = 0;
	private double timeOfAvgSync = 0.0;
	
	private double interval = 0.0;
	
	private ArrayList<Double> hist;
	
	private boolean fixed = false;
	private boolean continueAdjuste = false;
	
	private int secAdjusterIte = 0;
	
	public IntervalAdjuster() {

		this.fixed = false;
		this.hist = new ArrayList<Double>();

	}
	
	public int computeNewInterval(double prog, int iteNum, int startRepeatIte, boolean isGMM) {
		if(iteNum == 0) return 10;

		hist.add(prog);
		
		StringBuffer sb = new StringBuffer("computeNewInterval-" + iteNum);
		sb.append("\nhist=" + this.hist.toString());
		sb.append("\nold=" + prog);
		
		if (this.fixed && (iteNum < startRepeatIte-1 || iteNum > startRepeatIte+2)) {
			return (int)Math.round(this.interval);
		}
		
		if (iteNum <= startRepeatIte) {
			sb.append("\nInitialized");
			this.interval = 10;
		}else if (iteNum == startRepeatIte+1) {
			this.interval = 20;
		}else if (iteNum == 3){
			double firProg = hist.get(1)/20; //Interval100下的单位进度
			double secProg = hist.get(2)/20; //Interval200下的单位进度
			firProg = (isGMM ? 0-firProg : firProg);
			secProg = (isGMM ? 0-secProg : secProg);
			double slope = (secProg - firProg)/10; //斜率
			double intercept = (firProg-slope*10); //截距
			
			//根据公式，求导，求出间隔
			this.interval = Math.sqrt((intercept * timeOfAvgSync)/slope);
			
			sb.append("\n firProg = " + firProg);
			sb.append("\n secProg = " + secProg);
			sb.append("\n slope = " + slope);
			sb.append("\n intercept = " + intercept);
			
			this.fixed = true;
			
		}else if(iteNum == startRepeatIte+2){ //再次开启调同步间隔
			double firProg = (hist.get(iteNum-4) - hist.get(iteNum-2))/20; //Interval 100 下的单位进度
			double secProg = (hist.get(iteNum-4) - hist.get(iteNum-1))/20; //Interval 200 下的单位进度
			firProg = (isGMM ? 0-firProg : firProg);
			secProg = (isGMM ? 0-secProg : secProg);
			double slope = (secProg - firProg)/10; //斜率
			double intercept = (firProg-slope*10); //截距
			
			//根据公式，求导，求出间隔
			double a = slope; //一元二次方程的a
			double b = 2*(slope * timeOfAvgSync); //一元二次方程的b
			double c = intercept * timeOfAvgSync; //一元二次方程的c
			double delta=b*b-4*a*c;
			
			this.interval = (-b-Math.sqrt(delta))/(2*a);

			
			sb.append("\n firProg = " + firProg);
			sb.append("\n secProg = " + secProg);
			sb.append("\n slope = " + slope);
			sb.append("\n intercept = " + intercept);

		}
			
		sb.append("\nnew_interval=" + (int)Math.round(this.interval));
		
		LOG.info(sb.toString());
		
		
		return (int)Math.round(this.interval);
	}
	

	
	public void updateSyncTime(double syncTime) {
		this.timeOfAllSync += syncTime;
		this.numOfAllSync++;
		this.timeOfAvgSync = this.timeOfAllSync/this.numOfAllSync;
	}
	
}
package org.apache.hama.monitor;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.myhama.util.Counters;
import org.apache.hama.myhama.util.Counters.COUNTER;

public class JobMonitor {
	private static final Log LOG = LogFactory.getLog(JobMonitor.class);
		
	private ArrayList<Double> agg_list;
	//private ArrayList<Counters> counters_list;
	
	/**
	 * Construct a job monitor.
	 * @param iterNum #iteration
	 * @param taskNum #tasks
	 */
	public JobMonitor (int _iterNum, int _taskNum) {		
		this.agg_list = new ArrayList<Double>();
		//this.counters_list = new ArrayList<Counters>();
	}
	
	public synchronized void updateMonitor(int curIteNum, int taskId, 
			double aggTask, Counters counters) {
		if (this.agg_list.size() < curIteNum) { //first task
			this.agg_list.add(aggTask);
			//this.counters_list.add(counters);
		} else { //remaining tasks, just add the value
			int idx = curIteNum - 1;
			this.agg_list.set(idx, this.agg_list.get(idx)+aggTask);
			
			/*Counters acc_counters = this.counters_list.get(idx);
			for (Enum<?> name: COUNTER.values()) {
				acc_counters.addCounter(name, counters.getCounter(name));
			}*/
		}
	}
	
	public void computeAgg(int curIteNum) {
		if (curIteNum > 1) {
			int idx = curIteNum - 1;
			this.agg_list.set(idx, 
					this.agg_list.get(idx-1)+this.agg_list.get(idx));
		}
	}
	
	/**
	 * Return the global sum-aggregator.
	 * @param curIteNum
	 * @return
	 */
	public double getAgg(int curIteNum) {
		return this.agg_list.get(curIteNum-1);
	}
	
	public double getDeltaAgg(int curIteNum) {
		if (curIteNum < 2) {
			return 0.0;
		} else {
			return (getAgg(curIteNum-1)-getAgg(curIteNum));
		}
	}
	
	private String printCounterInfo(Enum<?> name) {
		StringBuffer sb = new StringBuffer();
		sb.append("\n");
		sb.append(name);
		sb.append("\n[");
		/*for (Counters counters: this.counters_list) {
			sb.append(counters.getCounter(name));
			sb.append(",");
		}*/
	    sb.append("0]");
	    return sb.toString();
	}
	
	public String printJobMonitor(int currIterNum) {
		StringBuffer sb = new StringBuffer();
		
		//sb.append(printCounterInfo(COUNTER.Vert_Read));
	    
	    return sb.toString();
	}
}

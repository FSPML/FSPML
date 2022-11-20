/**
 * copyright 2011-2016
 */
package hybridgraph.examples.fcm.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.io.EdgeParser;

/**
 * FCMUserTool.java
 * 
 * @author 
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class FCMUserTool 
		extends UserTool<FCMValue, Integer, Integer, Integer> {
	private static EdgeParser edgeParser = new EdgeParser();
	
	public static class FCMRecord 
			extends GraphRecord<FCMValue, Integer, Integer, Integer> {
		
		@Override
	    public void parseGraphData(String vData, String dimData) {
			this.verId = Integer.valueOf(vData);
			this.setDimensions(edgeParser.parseDimensionArray(dimData, ','));
			this.verValue = new FCMValue(this.getNumOfDimensions());			
	    }
		
	    @Override
		public void write(DataOutput out) throws IOException {
	    	out.writeInt(this.verId);
	    	int count = getNumOfDimensions();
	    	out.writeInt(count);
	    	for (int i = 0; i < count; i++) {
	    		out.writeDouble(this.dimensions[i]);
	    	}
	    	
	    	out.writeDouble(this.verValue.getError());
	    	int countOfMiu = this.verValue.getMiu().length;
	    	out.writeInt(countOfMiu);
	    	for(int i = 0; i < countOfMiu; i++) {
	    		out.writeDouble(this.verValue.getMiu()[i]);
	    	}
	    }
	    
	    @Override
		public void readFields(DataInput in) throws IOException {
	    	this.verId = in.readInt();
	    	int count = in.readInt();
	    	this.dimensions = new double[count];
	    	for (int i = 0; i < count; i++) {
	    		this.dimensions[i] = in.readDouble();
	    	}
	    	
	    	double error = in.readDouble();
	    	int countOfMiu = in.readInt();
	    	double[] miu = new double[countOfMiu];
	    	for (int i = 0; i < count; i++) {
	    		miu[i] = in.readDouble();
	    	}
	    	this.verValue.set(error, miu);
	    }
	}
	
	@Override
	public GraphRecord<FCMValue, Integer, Integer, Integer> 
			getGraphRecord() {
		return new FCMRecord();
	}
}

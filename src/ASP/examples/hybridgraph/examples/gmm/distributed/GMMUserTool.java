/**
 * copyright 2011-2016
 */
package hybridgraph.examples.gmm.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.io.EdgeParser;

/**
 * GMMUserTool.java
 * 
 * @author 
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class GMMUserTool 
		extends UserTool<GMMValue, Integer, Integer, Integer> {
	private static EdgeParser edgeParser = new EdgeParser();
	
	public static class GMMRecord 
			extends GraphRecord<GMMValue, Integer, Integer, Integer> {
		
		@Override
	    public void parseGraphData(String vData, String dimData) {
			this.verId = Integer.valueOf(vData);
			this.setDimensions(edgeParser.parseDimensionArray(dimData, ','));
			
			this.verValue = new GMMValue(this.getNumOfDimensions());
	    }
	    @Override
		public void write(DataOutput out) throws IOException {
	    	out.writeInt(this.verId);
	    	int count = getNumOfDimensions();
	    	out.writeInt(count);
	    	for (int i = 0; i < count; i++) {
	    		out.writeDouble(this.dimensions[i]);
	    	}
	    	
	    	int errorsCount = this.verValue.getErrors().length;
	    	out.writeInt(errorsCount);
	    	for (int i = 0; i < errorsCount; i++) {
	    		out.writeDouble(this.verValue.getErrors()[i]);
	    	}
	    	
	    	int relationsCount = this.verValue.getRelations().length;
	    	out.writeInt(relationsCount);
	    	for (int i = 0; i < errorsCount; i++) {
	    		out.writeDouble(this.verValue.getRelations()[i]);
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
	    	
	    	int errorsCount = in.readInt();
	    	double[] errors = new double[errorsCount];
	    	for (int i = 0; i < errorsCount; i++) {
				errors[i] = in.readDouble();
			}
	    	
	    	int relationsCount = in.readInt();
	    	double[] relations = new double[relationsCount];
	    	for (int i = 0; i < relationsCount; i++) {
	    		relations[i] = in.readDouble();
			}
	    	
	    	this.verValue = new GMMValue(errorsCount);
	    	this.verValue.set(errors, relations);
	    }
	}
	
	@Override
	public GraphRecord<GMMValue, Integer, Integer, Integer> 
			getGraphRecord() {
		return new GMMRecord();
	}
}

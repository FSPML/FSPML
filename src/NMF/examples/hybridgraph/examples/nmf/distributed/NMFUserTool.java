package hybridgraph.examples.nmf.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.io.EdgeParser;

/**
 * 
 * @author panchao dai
 */
public class NMFUserTool extends UserTool<NMFValue, Integer, Integer, Integer> {
	
	private static EdgeParser edgeParser = new EdgeParser();
	
	private static class NMFRecord extends GraphRecord<NMFValue, Integer, Integer, Integer> {
		
		@Override
		public void parseGraphData(String vData, String dimData) {
			this.verId = Integer.valueOf(vData);
			this.setDimensions(edgeParser.parseDimensionArray(dimData, ','));
			this.verValue = new NMFValue();
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
	    	out.writeInt(this.verId);
	    	int count = getNumOfDimensions();
	    	out.writeInt(count);
	    	for (int i = 0; i < count; i++) {
	    		out.writeDouble(this.dimensions[i]);
	    	}
	    	
	    	out.writeDouble(this.verValue.getLost());
	    }
		
		@Override
		public void readFields(DataInput in) throws IOException {
	    	this.verId = in.readInt();
	    	int count = in.readInt();
	    	this.dimensions = new double[count];
	    	for (int i = 0; i < count; i++) {
	    		this.dimensions[i] = in.readDouble();
	    	}
	    	
	    	double lost = in.readDouble();
	    	this.verValue = new NMFValue();
	    	this.verValue.setLost(lost);
	    }
	}

	@Override
	public GraphRecord<NMFValue, Integer, Integer, Integer> getGraphRecord() {
		return new NMFRecord();
	}
	
}

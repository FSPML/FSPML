/**
 * copyright 2011-2016
 */
package hybridgraph.examples.gmm.distributed;

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
	}
	
	@Override
	public GraphRecord<GMMValue, Integer, Integer, Integer> 
			getGraphRecord() {
		return new GMMRecord();
	}
}

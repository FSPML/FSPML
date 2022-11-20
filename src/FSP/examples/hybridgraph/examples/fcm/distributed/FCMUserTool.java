/**
 * copyright 2011-2016
 */
package hybridgraph.examples.fcm.distributed;

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
	}
	
	@Override
	public GraphRecord<FCMValue, Integer, Integer, Integer> 
			getGraphRecord() {
		return new FCMRecord();
	}
}

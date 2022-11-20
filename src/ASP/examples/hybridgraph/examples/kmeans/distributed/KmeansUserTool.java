/**
 * copyright 2011-2016
 */
package hybridgraph.examples.kmeans.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.io.EdgeParser;

/**
 * PageRankUserTool.java
 * Support for {@link PageRankGraphRecord} and {@link PageRankMsgRecord}.
 * 
 * @author 
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class KmeansUserTool 
		extends UserTool<KmeansValue, Integer, Integer, Integer> {
	private static EdgeParser edgeParser = new EdgeParser();
	
	public static class KmeansRecord 
			extends GraphRecord<KmeansValue, Integer, Integer, Integer> {
		
		@Override
	    public void parseGraphData(String vData, String dimData) {
			this.verId = Integer.valueOf(vData);
			this.verValue = new KmeansValue();
			this.verValue.set(this.verId, Double.MAX_VALUE);
			
			this.setDimensions(edgeParser.parseDimensionArray(dimData, ','));
	    }
	}
	
	@Override
	public GraphRecord<KmeansValue, Integer, Integer, Integer> 
			getGraphRecord() {
		return new KmeansRecord();
	}
}

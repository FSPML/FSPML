package hybridgraph.examples.logR.distributed;

import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.io.EdgeParser;


public class LogRUserTool extends UserTool<LogRValue, Integer, Integer, Integer>{
	private static EdgeParser edgeParser = new EdgeParser();
	
	public static class LogRRecord extends GraphRecord<LogRValue, Integer, Integer, Integer>{
		
		
		@Override
		public void parseGraphData(String vData, String dimData) {
			this.verId = Integer.valueOf(vData);
			this.setDimensions(edgeParser.parseDimensionArray(dimData, ','));
			double[] dim = this.getDimensions();
			double label = dim[(this.getNumOfDimensions() - 1)];
			this.verValue = new LogRValue();
		}
	}
	

	@Override
	public GraphRecord<LogRValue, Integer, Integer, Integer> getGraphRecord() {
		// TODO Auto-generated method stub
		return new LogRRecord();
	}
}

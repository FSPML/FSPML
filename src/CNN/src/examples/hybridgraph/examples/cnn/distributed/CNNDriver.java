package hybridgraph.examples.cnn.distributed;

import org.apache.hadoop.fs.Path;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.Constants.SyncModel;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.io.KeyValueInputFormat;
import org.apache.hama.myhama.io.TextBSPFileOutputFormat;

import hybridgraph.examples.kmeans.distributed.KmeansBSP;
import hybridgraph.examples.kmeans.distributed.KmeansDriver;
import hybridgraph.examples.kmeans.distributed.KmeansUserTool;

public class CNNDriver {
	public static void main(String[] args) throws Exception {
		//check the input parameters
		if (args.length != 10) {
			StringBuffer sb = 
				new StringBuffer("the K-means job must be given arguments(12):"); 
			sb.append("\n  [1] input directory on HDFS"); 
			sb.append("\n  [2] output directory on HDFS"); 
			sb.append("\n  [3] #task(int)");
			sb.append("\n  [4] #iteration (int)");
			sb.append("\n  [5] #points");
			sb.append("\n  [6] SyncModel (int, Concurrent => 1, Block => 2, SemiAsyn => 3)");
			sb.append("\n  [7] #block_size (int, #points)");
			sb.append("\n  [8] #barrier_interval (int, milliseconds)");
			sb.append("\n  [9] #idle_time per 1000 points (int, milliseconds, -1=>disabled)");
			sb.append("\n  [10] data load skew (double, 0.0<=skew<1.0)");
			
			System.out.println(sb.toString());
			System.exit(-1);
		}
		
		//set the job configuration
		HamaConfiguration conf = new HamaConfiguration();
		BSPJob bsp = new BSPJob(conf, KmeansDriver.class);
		bsp.setPriority(Constants.PRIORITY.NORMAL);
		bsp.setJobName("Kmeans");
		
		bsp.setBspClass(KmeansBSP.class);
		bsp.setUserToolClass(KmeansUserTool.class);
		bsp.setInputFormatClass(KeyValueInputFormat.class);
		bsp.setOutputFormatClass(TextBSPFileOutputFormat.class);
		
		KeyValueInputFormat.addInputPath(bsp, new Path(args[0]));
		TextBSPFileOutputFormat.setOutputPath(bsp, new Path(args[1]));
		bsp.setNumBspTask(Integer.parseInt(args[2]));
		bsp.setNumSuperStep(Integer.parseInt(args[3]));
		bsp.setNumTotalVertices(Integer.valueOf(args[4]));
		
		switch(Integer.parseInt(args[5])) {
		case 1: 
			bsp.setSyncModel(SyncModel.Concurrent);
			break;
		case 2:
			bsp.setSyncModel(SyncModel.Block);
			bsp.setBlockSize(Integer.valueOf(args[6]));
			break;
		case 3:
			bsp.setSyncModel(SyncModel.SemiAsyn);
			bsp.setBarrierInterval(Integer.valueOf(args[7]));
		}
		
		bsp.setIdleTime(Integer.parseInt(args[8]));
		bsp.setDataLoadSkew(args[9]);
		
		//submit the job
		bsp.waitForCompletion(true);
	}
}

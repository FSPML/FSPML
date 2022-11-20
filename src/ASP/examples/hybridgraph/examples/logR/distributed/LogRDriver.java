package hybridgraph.examples.logR.distributed;

import org.apache.hadoop.fs.Path;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.Constants.SyncModel;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.io.KeyValueInputFormat;
import org.apache.hama.myhama.io.TextBSPFileOutputFormat;

public class LogRDriver {
	public static void main(String[] args) throws Exception {
		//check the input parameters
		if (args.length != 16) {
			StringBuffer sb = 
					new StringBuffer("the logistic regression job must be given arguments(16):");
			sb.append("\\n  [1] input directory on HDFS");
			sb.append("\\n  [2] output directory on HDFS");
			sb.append("\n  [3] #task(int)"); //task数目
			sb.append("\n  [4] #iteration (int)");//迭代
			sb.append("\n  [5] #points");//样本数
			sb.append("\n  [6] #rate (float, for logR)");//学习率
			sb.append("\n  [7] #initTheta (float, for logR)");
			sb.append("\n  [8] #dimensions (int)");//样本维度，包含Y
			sb.append("\n  [9] SyncModel (int, Concurrent => 1, Block => 2, SemiAsyn => 3)");
			sb.append("\n  [10] #block_size (int, #points)");//块大小
			sb.append("\n  [11] #barrier_interval_ratio (double, 0<ratio<=1.0)");
			sb.append("\n  [12] #idle_time per 1000 points (int, milliseconds, -1=>disabled, -2=>Multithreading simulation)");
			sb.append("\n  [13] #idle_task (int, 0=>disabled)");
			sb.append("\n  [14] converge_threshold (double)");
			sb.append("\n  [15] data load skew (double, 0.0<=skew<1.0)");
			sb.append("\n  [16] load migration ? (int, 1 => yes ,-1 => no, 2=>block)");
			System.out.println(sb.toString());
			System.exit(-1);
		}
		
		//set the job configuration 
		HamaConfiguration conf = new HamaConfiguration();
		BSPJob bsp = new BSPJob(conf, LogRDriver.class);
		bsp.setPriority(Constants.PRIORITY.NORMAL);
		bsp.setJobName("logR");
		
		bsp.setBspClass(LogRBSP.class);
		bsp.setUserToolClass(LogRUserTool.class);
		
		bsp.setInputFormatClass(KeyValueInputFormat.class);
		bsp.setOutputFormatClass(TextBSPFileOutputFormat.class);
		
		KeyValueInputFormat.addInputPath(bsp, new Path(args[0]));
		TextBSPFileOutputFormat.setOutputPath(bsp, new Path(args[1]));
		bsp.setNumBspTask(Integer.parseInt(args[2]));
		bsp.setNumSuperStep(Integer.parseInt(args[3]));
		bsp.setNumTotalVertices(Integer.valueOf(args[4]));
		bsp.setLearnRate(Float.valueOf(args[5])); //rate
		bsp.setInitTheta(Float.valueOf(args[6])); //initTheta
		bsp.setNumOfDimensions(Integer.valueOf(args[7]));
		switch(Integer.parseInt(args[8])) {
		case 1: 
			bsp.setSyncModel(SyncModel.Concurrent);
			break;
		case 2:
			bsp.setSyncModel(SyncModel.Block);
			bsp.setBlockSize(Integer.valueOf(args[9]));
			break;
		case 3:
			bsp.setSyncModel(SyncModel.SemiAsyn);
			bsp.setBarrierIntervalRatio(args[10]);
		}
		bsp.setIdleTime(Integer.parseInt(args[11]));
		bsp.setNumOfIdleTasks(Integer.parseInt(args[12]));
		bsp.setConvergeValue(args[13]);
		bsp.setLOGR();
		bsp.setDataLoadSkew(args[14]);
		bsp.setIsMigration(Integer.parseInt(args[15]));
		
		//submit the job
		bsp.waitForCompletion(true);
	}
}

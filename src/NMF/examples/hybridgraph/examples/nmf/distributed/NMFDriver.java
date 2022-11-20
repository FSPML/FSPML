package hybridgraph.examples.nmf.distributed;

import org.apache.hadoop.fs.Path;
import org.apache.hama.Constants;
import org.apache.hama.Constants.SyncModel;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.io.KeyValueInputFormat;
import org.apache.hama.myhama.io.TextBSPFileInputFormat;
import org.apache.hama.myhama.io.TextBSPFileOutputFormat;

/**
 * 
 * @author panchao dai
 */
public class NMFDriver {
	public static void main(String[] args) throws Exception {
		
		// check the input parameters
		if (args.length != 13) {
			StringBuffer stringBuffer = 
					new StringBuffer("the nmf regression job must be given arguments(13):");
			stringBuffer.append("\\n  [1] input directory on HDFS");
			stringBuffer.append("\\n  [2] output directory on HDFS");
			stringBuffer.append("\n  [3] #task (int)"); // task 数目
			stringBuffer.append("\n  [4] #iteration (int)"); // 迭代次数
			stringBuffer.append("\n  [5] #points"); // 样本数
			stringBuffer.append("\n  [6] #dimensions (int)"); // 样本维度（传入矩阵的列数）
			stringBuffer.append("\n  [7] SyncModel (int, Concurrent => 1, Block => 2, SemiAsyn => 3)"); // 3
			stringBuffer.append("\n  [8] #block_size (int, #points)"); // -1
			stringBuffer.append("\n  [9] #barrier_interval_ratio (double, 0<ratio<=1.0)"); // 1.0
			stringBuffer.append("\n  [10] #idle_time per 1000 points (int, milliseconds, -1=>disabled)"); // -1
			stringBuffer.append("\n  [11] #idle_task (int, 0=>disabled)"); // 0
			stringBuffer.append("\n  [12] converge_threshold (double)"); // 阈值
			stringBuffer.append("\n  [13] data load skew (double, 0.0<=skew<1.0)"); // 0.0
			
			System.out.println(stringBuffer.toString());
			System.exit(-1);
		}
		
		// set the job configuration
		HamaConfiguration configuration = new HamaConfiguration();
		BSPJob bspJob = new BSPJob(configuration, NMFDriver.class);
		bspJob.setPriority(Constants.PRIORITY.NORMAL);
		bspJob.setJobName("nmf");
		
		bspJob.setBspClass(NMFBSP.class);
		bspJob.setUserToolClass(NMFUserTool.class);
		bspJob.setInputFormatClass(KeyValueInputFormat.class);
		bspJob.setOutputFormatClass(TextBSPFileOutputFormat.class);
		
		KeyValueInputFormat.addInputPath(bspJob, new Path(args[0]));
		TextBSPFileOutputFormat.setOutputPath(bspJob, new Path(args[1]));
		bspJob.setNumBspTask(Integer.parseInt(args[2]));
		bspJob.setNumSuperStep(Integer.parseInt(args[3]));
		bspJob.setNumTotalVertices(Integer.parseInt(args[4]));
		bspJob.setNumOfDimensions(Integer.valueOf(args[5]));
		
		switch (Integer.parseInt(args[6])) {
		case 1:
			bspJob.setSyncModel(SyncModel.Concurrent);
			break;
		case 2:
			bspJob.setSyncModel(SyncModel.Block);
			bspJob.setBlockSize(Integer.valueOf(args[7]));
			break;
		case 3:
			bspJob.setSyncModel(SyncModel.SemiAsyn);
			bspJob.setBarrierIntervalRatio(args[8]);
			break;
		}
		
		bspJob.setIdleTime(Integer.parseInt(args[9]));
		bspJob.setNumOfIdleTasks(Integer.parseInt(args[10]));
		bspJob.setConvergeValue(args[11]);
		bspJob.setNMF();
		bspJob.setDataLoadSkew(args[12]);
		
		// submit the job
		bspJob.waitForCompletion(true);
	}
}

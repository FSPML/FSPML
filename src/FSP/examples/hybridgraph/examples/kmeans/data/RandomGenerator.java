package hybridgraph.examples.kmeans.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Random;

/**
 * Data generator for K-means algorithm used in SemiAsyn.
 * The output dataset follows the format:
 * <code>point_id</code>\t<code>dimension_1,dimension_2,...</code>
 * 
 * The input args includes:
 *   (1)#points
 *   (2)#dimensions
 *   (3)#output file
 * 
 * After generating, this program reports the following info.:
 * (1) size(MB);
 * 
 * @author Zhigang Wang
 * @time 2016.11.20
 */
public class RandomGenerator {
	// the total number of points
	public static int POINT_NUM = 10;
	// the number of dimensions
	public static int DIMENSION_NUM = 2;
	
	// define the split flag
	public static String POINT_DIM_SPLIT = "\t";
	public static String DIM_DIM_SPLIT = ",";
	// the output path
	public static String OUTPUT_PATH = "/termite-rd-kmeans";
	
	public static void main(String[] args) {
		// check the input arguments
		if (args.length < 2) {
			StringBuffer sb = 
				new StringBuffer("input arguments of K-means dataset generator:");
			sb.append("\n\t[1]"); sb.append("points: #points(int, >1).");
			sb.append("\n\t[2]"); sb.append("dimensions: #dimensions(int, >1).");
			
			System.out.println(sb.toString());
			System.exit(-1);
		} else {
			POINT_NUM = Integer.valueOf(args[0]);
			DIMENSION_NUM = Integer.valueOf(args[1]);
			OUTPUT_PATH = "kmeans-rd-point" + args[0] + "-dim" + args[1];
		}
		
		Random dimGenerator = new Random();
		File root = new File(OUTPUT_PATH);
		try{
			System.out.println("begin to generate " + OUTPUT_PATH + ", please wait...");
			FileWriter fw = new FileWriter(root);
			BufferedWriter bw = new BufferedWriter(fw);
			
			StringBuffer sb = new StringBuffer();
			for (int pointCount = 0; pointCount < POINT_NUM; pointCount++) {
				sb.setLength(0);
				sb.append(Integer.toString(pointCount));
				sb.append(POINT_DIM_SPLIT);
				
				sb.append(Double.toString(dimGenerator.nextDouble()));
				for (int dimCount = 1; dimCount < DIMENSION_NUM; dimCount++) {
					sb.append(DIM_DIM_SPLIT);
					sb.append(Double.toString(dimGenerator.nextDouble()));
				}
				bw.write(sb.toString());
				bw.newLine();
			}
			
			bw.close();
			fw.close();
		}catch(Exception e){
			System.out.println(e);
		}	
		
		System.out.println("generate data successfully!");
		System.out.println(OUTPUT_PATH + ", " 
				+ root.length()/(float)(1024 * 1024) + " MB");
	}
}

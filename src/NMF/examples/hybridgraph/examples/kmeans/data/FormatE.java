package hybridgraph.examples.kmeans.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * Format the raw data by transforming xxEyy into a double value, removing 
 * the label per record, and finally adding point_id for each record.
 * 
 * (format HIGGS.csv, http://archive.ics.uci.edu/ml/datasets/HIGGS#)
 * 
 * The input data format:
 * label,dimension_1,dimension_2,dimension_3,...
 * 
 * The output data format:
 * <code>point_id</code>\t<code>dimension_1,dimension_2,...</code>
 * 
 * The input args includes:
 *   (1)input file path
 * 
 * After generating, this program reports the following info.:
 * (1) size(MB);
 * (2) #points;
 * 
 * @author Zhigang Wang
 * @time 2016.12.14
 */
public class FormatE {
	
	public static void main(String[] args) throws Exception {
		String input = null, output = null;
		args = new String[]{"/root/桌面/share/主机_桌面/Semi-asynchronous/experiments/dataset/HIGGS/HIGGS.csv"};
		
		if (args.length != 1) {
			System.out.println("Usage: [input]");
			System.exit(-1);
		} else {
			input = args[0];
			output = input + "-format";
		}
		
		File inputF = new File(input);
		FileReader fr = new FileReader(inputF);
		BufferedReader br = new BufferedReader(fr);
		
		File outputF = new File(output);
		FileWriter fw = new FileWriter(outputF);
		BufferedWriter bw = new BufferedWriter(fw);
		
		String context = null;
		int pointId = 0;
		StringBuffer sb = new StringBuffer();
		System.out.println("begin to format, please wait...");
		while((context=br.readLine()) != null) {
			sb.setLength(0);
			sb.append(Integer.toString(pointId++));
			sb.append("\t");
			
			String[] strs = context.split(",");
			double dim = Double.valueOf(strs[1]);
			sb.append(dim);
			for (int i = 2; i < strs.length; i++) {
				sb.append(",");
				dim = Double.valueOf(strs[i]);
				sb.append(dim);
			}
			
			bw.write(sb.toString());
			bw.newLine();
			if (pointId%10000 == 0) {
				System.out.println("progress: " + pointId);
			}
		}
		
		br.close();
		fr.close();
		bw.close();
		fw.close();
		
		System.out.println("format data successfully!");
		System.out.println(output + ", " 
				+ outputF.length()/(float)(1024 * 1024) + " MB");
		System.out.println("#points=" + pointId);
	}
}

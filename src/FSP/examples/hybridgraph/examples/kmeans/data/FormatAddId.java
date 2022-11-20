package hybridgraph.examples.kmeans.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * Format the raw data by adding point id.
 * 
 * The input data format:
 * dimension_1,dimension_2,dimension_3,...
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
 * @time 2016.11.22
 */
public class FormatAddId {
	
	public static void main(String[] args) throws Exception {
		String input = null, output = null;
		
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
		System.out.println("begin to add point id, please wait...");
		while((context=br.readLine()) != null) {
			sb.setLength(0);
			sb.append(Integer.toString(pointId++));
			sb.append("\t");
			sb.append(context);
			bw.write(sb.toString());
			bw.newLine();
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

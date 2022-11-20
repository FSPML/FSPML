package hybridgraph.examples.kmeans.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Format the raw data by replacing string dimension values with integer values.
 * 
 * The input data format:
 * dimension_1,dimension_2,dimension_3,...
 * 
 * The output data format:
 * dimension_1,dimension_2,...
 * 
 * The input args includes:
 *   (1)input file path
 * 
 * After generating, this program reports the following info.:
 * (1) size(MB);
 * 
 * @author Zhigang Wang
 * @time 2016.11.22
 */
public class FormatReplacement {
	
	public static void main(String[] args) throws Exception {
		String input = null, output = null;
		int[] repDimIdx = null;
		
		if (args.length != 2) {
			System.out.println("Usage: [input] [dimension_idx_replacement, e.g., 0:2:5:7]");
			System.exit(-1);
		} else {
			input = args[0];
			output = input + "-replacement";
			String[] idxes = args[1].split(":");
			repDimIdx = new int[idxes.length];
			for (int i = 0; i < idxes.length; i++) {
				repDimIdx[i] = Integer.parseInt(idxes[i]);
			}
		}
		
		File inputF = new File(input);
		FileReader fr = new FileReader(inputF);
		BufferedReader br = new BufferedReader(fr);
		
		String context = null;
		HashMap<String, Integer> names = new HashMap<String, Integer>();
		System.out.println("begin to collect dimension names, please wait...");
		while((context=br.readLine()) != null) {
			String[] dims = context.split(",");
			for (int i = 0; i < repDimIdx.length; i++) {
				String key = dims[repDimIdx[i]];
				if (names.containsKey(key)) {
					names.put(key, names.get(key)+1);
				} else {
					names.put(key, 1);
				}
			}
		}
		br.close();
		fr.close();
		System.out.println("enumerate all string names and frequencies:");
		String[] keys = new String[names.size()];
		int keyIdx = 0;
		for (Entry<String, Integer> entry: names.entrySet()) {
			System.out.println(entry.getKey() + ", " + entry.getValue());
			keys[keyIdx++] = entry.getKey();
		}
		System.out.println("replacement policy:");
		HashMap<String, String> replacement = new HashMap<String, String>();
		for (keyIdx = 0; keyIdx < keys.length; keyIdx++) {
			replacement.put(keys[keyIdx], Integer.toString(keyIdx+1));
		}
		for (Entry<String, String> entry: replacement.entrySet()) {
			System.out.println(entry.getKey() + " => " + entry.getValue());
		}
		
		fr = new FileReader(inputF);
		br = new BufferedReader(fr);
		
		File outputF = new File(output);
		FileWriter fw = new FileWriter(outputF);
		BufferedWriter bw = new BufferedWriter(fw);
		StringBuffer sb = new StringBuffer();
		System.out.println("begin to replace sting values, please wait...");
		while((context=br.readLine()) != null) {
			sb.setLength(0);
			String[] dims = context.split(",");
			for (int i = 0; i < repDimIdx.length; i++) {
				dims[repDimIdx[i]] = replacement.get(dims[repDimIdx[i]]);
			}
			
			sb.append(dims[0]);
			for (int i = 1; i < dims.length; i++) {
				sb.append(",");
				sb.append(dims[i]);
			}
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.close();
		fw.close();
		
		System.out.println("replacing is done successfully!");
		System.out.println(output + ", " 
				+ outputF.length()/(float)(1024 * 1024) + " MB");
	}
}

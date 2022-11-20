package hybridgraph.examples.nmf.single;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class Reading {

	public static void main(String[] args) throws IOException {
		String path = "D:\\NMF数据集\\ml-10m-ratings";
		BufferedReader bufferedReader = null;
		
		try {
			bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		String line = "";
		int count = 0;
		
		while ((line = bufferedReader.readLine()) != null) {
			count++;
			System.out.println(count);
		}
	}

}

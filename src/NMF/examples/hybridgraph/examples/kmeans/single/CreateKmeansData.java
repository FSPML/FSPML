package hybridgraph.examples.kmeans.single;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

/**
 * 该类用于生成自己测试用的数据
 * 
 * @author panchao dai
 */
public class CreateKmeansData {
	private static int N = 3000000, M = 34;
	private static Random random = new Random();

	public static void main(String[] args) throws IOException {
		File file = new File("D:\\data\\kmeans-06");
		OutputStream outputStream = new FileOutputStream(file);
		double num;
		
        for (int i = 0; i < N; i++) {
        	System.out.println(i);
        	// 先写入编号
        	outputStream.write(String.valueOf(i).getBytes());
        	outputStream.write("\t".getBytes());
        	
        	// 后写入数据
        	for (int j = 0; j < M; j++) {
        		num = random.nextDouble() * 10.0 + random.nextDouble();
        		outputStream.write(String.valueOf(num).getBytes());
        		if (j < M - 1) {
					outputStream.write(",".getBytes());
				}
        	}
        	
        	outputStream.write("\n".getBytes());
        }
	}
}

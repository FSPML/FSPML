package hybridgraph.examples.nmf.single;

import java.io.*;

/**
 * 测试使用
 */
public class ReadTest {

	private static final int N = 100000;
	private static boolean[] is = new boolean[N];
	
	public static void main(String[] args) throws FileNotFoundException {
		String path = "D:\\NMF数据集\\ml-10m\\ml-10M100K\\ratings.dat";
		BufferedReader bufferedReader = null;
		
		try {
			bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		for (int i = 0; i < N; i++) {
			is[i] = false;
		}
		
		String line = "";
		int count = 0;
		try {
			while ((line = bufferedReader.readLine()) != null) {
				int movieId = getMovieId(line);
				System.out.println(movieId);
				if (!is[movieId]) {
					is[movieId] = true;
					count++;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("count = " + count);
	}
	
	/**
	 * 得到电影 id
	 * @param line
	 * @return
	 */
	public static int getMovieId(String line) {
		int  movieId = 0;
		int id = 0;
		
		while (id < line.length()) {
			if (line.charAt(id) == ':') {
				break;
			}
			id++;
		}
		id += 2;
		
		while (id < line.length() && line.charAt(id) != ':') {
			movieId = movieId * 10 + (line.charAt(id) - '0');
			id++;
		}
		
		return movieId;
	}

}

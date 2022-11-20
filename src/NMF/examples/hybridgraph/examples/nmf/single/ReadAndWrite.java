package hybridgraph.examples.nmf.single;

import java.io.*;

public class ReadAndWrite {
	
	private static double[] ratings = new double[1070000];
	
	public static void main(String[] args) throws FileNotFoundException {
		init();
		String path = "D:\\NMF数据集\\ml-10m\\ml-10M100K\\ratings.dat";
		BufferedReader bufferedReader = null;
		
		try {
			bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		String line = "";
		int userId = 1;
		int preUserId = 1;
		int movieId = 1;
		double rating = 0.0;
		File file = new File("D:\\NMF数据集\\ml-10m-ratings");
		OutputStream outputStream = new FileOutputStream(file);
		
		int count = 0;
		try {
			while ((line = bufferedReader.readLine()) != null) {
				userId = getUserId(line);
				movieId = getMovieId(line);
				rating = getRating(line);
				
				if (userId == preUserId) {
					ratings[movieId] = rating;
				} else {
					System.out.println(count);
					outputStream.write(String.valueOf(count).getBytes());
					outputStream.write("\t".getBytes());
					
					for (int i = 1; i <= 10681; i++) {
						if (i > 1) {
							outputStream.write(",".getBytes());
						}
						outputStream.write(String.valueOf(ratings[i]).getBytes());
						ratings[i] = 0.01;
					}
					ratings[movieId] = rating;
					
					outputStream.write("\n".getBytes());
					preUserId = userId;
					count++;
				}
			}
			
			// 最后一行
			System.out.println(count);
			outputStream.write(String.valueOf(count).getBytes());
			outputStream.write("\t".getBytes());
			for (int i = 1; i <= 10681; i++) {
				if (i > 1) {
					outputStream.write(",".getBytes());
				}
				outputStream.write(String.valueOf(ratings[i]).getBytes());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void init() {
		for (int i = 0; i < 1070000; i++) {
			ratings[i] = 0.01;
		}
	}
	
	/**
	 * 得到用户 id
	 * @param line
	 * @return
	 */
	public static int getUserId(String line) {
		int userId = 0;
		
		for (int i = 0; i < line.length(); i++) {
			if (line.charAt(i) == ':') break;
			userId = userId * 10 + (line.charAt(i) - '0');
		}
		
		return userId;
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
	
	/**
	 * 得到评分
	 * @param line
	 * @return
	 */
	public static double getRating(String line) {
		double  rating = 0;
		int id = 0;
		
		while (id < line.length()) {
			if (line.charAt(id) == ':') {
				break;
			}
			id++;
		}
		id += 2;
		
		while (id < line.length()) {
			if (line.charAt(id) == ':') {
				break;
			}
			id++;
		}
		id += 2;
		
		while (id < line.length() && line.charAt(id) != ':' && line.charAt(id) != '.') {
			rating = rating * 10.0 + (line.charAt(id) - '0') * 1.0;
			id++;
		}
		
		double base = 10.0;
		if (line.charAt(id) == '.') {
			id++;
			while (id < line.length() && line.charAt(id) != ':') {
				rating = rating + (line.charAt(id) - '0') / base;
				base = base / 10.0;
				id++;
			}
		}
		
		return rating;
	}

}

package hybridgraph.examples.kmeans.single;

import java.io.*;

public class SplitData {
    public static void main(String[] args) throws Exception {
        // 读入数据
        String path = "D:\\data\\HIGGS.csv-format-em";
        BufferedReader bufferedReader = null;

        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String line;
        int count = 0;
        // 需要上传集群的文件
        File file = new File("D:\\data\\HIGGS_colony");
        OutputStream outputStream = new FileOutputStream(file);

        while ((line = bufferedReader.readLine()) != null) {
            System.out.println(count);
            outputStream.write(line.getBytes());
            outputStream.write("\n".getBytes());
            count++;
            if (count >= 4350930) break;
        }


        // master本地文件
        file = new File("D:\\data\\HIGGS_master_local");
        outputStream = new FileOutputStream(file);

        while ((line = bufferedReader.readLine()) != null) {
            System.out.println(count);
            outputStream.write(removeId(line).getBytes());
            outputStream.write("\n".getBytes());
            count++;
        }
    }

    private static String removeId(String line) {
        String str = "";
        boolean flag = false;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == '\t') {
                flag = true;
                continue;
            }
            if (flag) {
                str += line.charAt(i);
            }
        }
        return str;
    }

}

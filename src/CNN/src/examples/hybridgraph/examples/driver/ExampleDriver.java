package hybridgraph.examples.driver;

import hybridgraph.examples.cnn.distributed.CNNDriver;
import hybridgraph.examples.fcm.single.FCMTest;
import hybridgraph.examples.gmm.distributed.GMMDriver;
import hybridgraph.examples.gmm.single.GMMTest;
import hybridgraph.examples.kmeans.data.FormatAddId;
import hybridgraph.examples.kmeans.data.FormatReplacement;
import hybridgraph.examples.kmeans.data.RandomGenerator;
import hybridgraph.examples.kmeans.distributed.KmeansDriver;
import hybridgraph.examples.kmeans.single.KmeansTest;
import hybridgraph.examples.logR.distributed.LogRDriver;


import org.apache.hadoop.util.ProgramDriver;


public class ExampleDriver {
	
	public static void main(String argv[]){
	    int exitCode = -1;
	    ProgramDriver pgd = new ProgramDriver();
	    
	    try {
	    	/** generate random dataset for K-means ΪK��ֵ����������ݼ� */
	    	pgd.addClass("kmeans.data.generator", RandomGenerator.class, 
    			"\tK-means random dataset generator (11/20/2016)");
	    	
	    	/** format the dataset for K-means by adding point ids ͨ����ӵ�ID����ʽ��K-means�����ݼ� */
	    	pgd.addClass("kmeans.data.format.addId", FormatAddId.class, 
    			"\tformat dataset for K-means by adding point ids (11/22/2016)");
	    	
	    	/** format dataset for K-means by replacing string values ͨ���滻�ַ���ֵ��ʽ��K-means�����ݼ� */
	    	pgd.addClass("kmeans.data.format.replace", FormatReplacement.class, 
    			"\tformat dataset for K-means by replacing string values (11/22/2016)");
	    	
	    	/** single-machine K-means for testing (concurrent/block) ��������ƽ���� ������/�飩*/
	    	pgd.addClass("kmeans.single", KmeansTest.class, 
    			"\tsingle-machine K-means impl. for testing (11/21/2016)");
	    	
	    	/** distributed K-means */
	    	pgd.addClass("kmeans.semiasyn", KmeansDriver.class, 
    			"\tdistributed K-means impl. under CONCURRENT or BLOCK model (11/20/2016)");
	    	
	    	/** single-machine GMM for testing */
	    	pgd.addClass("gmm.single", GMMTest.class, 
    			"\tsingle-machine GMM impl. for testing (02/08/2017)");
	    	
	    	/** distributed GMM */
	    	pgd.addClass("gmm.semiasyn", GMMDriver.class, 
    			"\tdistributed GMM impl. under CONCURRENT or BLOCK model (02/08/2017)");
	    	
	    	/** single-machine Fuzzy C-Means for testing */
	    	pgd.addClass("fcm.single", FCMTest.class, 
    			"\tsingle-machine FCM impl. for testing (02/09/2017)");
	    	
	    	
	    	/** distributed SGD */
	    	pgd.addClass("logr.semiasyn", LogRDriver.class, 
    			"\tdistributed logr impl. under CONCURRENT or SemiAsyn model (05/07/2021)");
	    	/** distributed K-means */
	    	pgd.addClass("cnn.semiasyn", CNNDriver.class, 
    			"\tdistributed K-means impl. under CONCURRENT or BLOCK model (11/20/2016)");
	    	
	    	pgd.driver(argv);
	    	exitCode = 0;
	    } catch(Throwable e) {
	    	e.printStackTrace();
	    }
	    
	    System.exit(exitCode);
	}
}

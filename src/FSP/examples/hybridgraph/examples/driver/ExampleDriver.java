package hybridgraph.examples.driver;

import hybridgraph.examples.fcm.distributed.FCMDriver;
import hybridgraph.examples.fcm.single.FCMTest;
import hybridgraph.examples.gmm.distributed.GMMDriver;
import hybridgraph.examples.gmm.single.GMMTest;
import hybridgraph.examples.kmeans.data.FormatAddId;
import hybridgraph.examples.kmeans.data.FormatReplacement;
import hybridgraph.examples.kmeans.data.RandomGenerator;
import hybridgraph.examples.kmeans.distributed.KmeansDriver;
import hybridgraph.examples.kmeans.single.KmeansTest;

import org.apache.hadoop.util.ProgramDriver;


public class ExampleDriver {
	
	public static void main(String argv[]){
	    int exitCode = -1;
	    ProgramDriver pgd = new ProgramDriver();
	    
	    try {
	    	/** generate random dataset for K-means */
	    	pgd.addClass("kmeans.data.generator", RandomGenerator.class, 
    			"\tK-means random dataset generator (11/20/2016)");
	    	
	    	/** format the dataset for K-means by adding point ids */
	    	pgd.addClass("kmeans.data.format.addId", FormatAddId.class, 
    			"\tformat dataset for K-means by adding point ids (11/22/2016)");
	    	
	    	/** format dataset for K-means by replacing string values */
	    	pgd.addClass("kmeans.data.format.replace", FormatReplacement.class, 
    			"\tformat dataset for K-means by replacing string values (11/22/2016)");
	    	
	    	/** single-machine K-means for testing */
	    	pgd.addClass("kmeans.single", KmeansTest.class, 
    			"\tsingle-machine K-means impl. for testing (11/21/2016)");
	    	
	    	/** distributed K-means */
	    	pgd.addClass("kmeans.semiasyn", KmeansDriver.class, 
    			"\tdistributed K-means impl. under semi-asynchronous model (11/20/2016)");
	    	
	    	/** single-machine GMM for testing */
	    	pgd.addClass("gmm.single", GMMTest.class, 
    			"\tsingle-machine GMM impl. for testing (02/09/2017)");
	    	
	    	/** distributed GMM */
	    	pgd.addClass("gmm.semiasyn", GMMDriver.class, 
    			"\tdistributed GMM impl. under CONCURRENT or SemiAsyn model (02/09/2017)");
	    	
	    	/** single-machine Fuzzy C-Means for testing */
	    	pgd.addClass("fcm.single", FCMTest.class, 
    			"\tsingle-machine FCM impl. for testing (02/09/2017)");
	    	
	    	/** distributed GMM */
	    	pgd.addClass("fcm.semiasyn", FCMDriver.class, 
    			"\tdistributed FCM impl. under CONCURRENT or SemiAsyn model (02/09/2017)");
	    	
	    	pgd.driver(argv);
	    	exitCode = 0;
	    } catch(Throwable e) {
	    	e.printStackTrace();
	    }
	    
	    System.exit(exitCode);
	}
}

package hybridgraph.examples.kmeans.distributed;

import com.aparapi.Kernel;
import com.aparapi.Range;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.util.AggregatorOfKmeans;
import org.apache.hama.myhama.util.Context;

public class KmeansUpdateOnGpu {
	private static final Log LOG = LogFactory.getLog(KmeansUpdateOnGpu.class);

	public KmeansUpdateOnGpu() {}

    public static void updateOnGpu(Context<KmeansValue, Integer, Integer, Integer> context) throws Exception {
		int[] numOfCentroids = new int[]{context.getBSPJobInfo().getNumOfCenters()};
		int[] numOfDimensions = new int[]{context.getBSPJobInfo().getNumOfDimensions()};
        GraphRecord<KmeansValue, Integer, Integer, Integer>[] centroids = context.getCentroids();

		Kernel kernel = new Kernel() {
			@Override
			public void run() {
				LOG.info("gpu hahaha");
				GraphRecord<KmeansValue, Integer, Integer, Integer> point = context.getGraphRecord();
				AggregatorOfKmeans[] aggregators = context.getAggregators();

				KmeansValue lastValue = point.getVerValue();
				KmeansValue curValue = new KmeansValue();
				int tag = -1;
				double minDist = Double.MAX_VALUE;

				for (int i = 0; i < numOfCentroids[0]; i++) {
					double dist = computeDistance(numOfDimensions[0],
							point.getDimensions(),
							centroids[i].getDimensions());
					//dist = 0.0f;
					if (dist < minDist) {
						minDist = dist;
						tag = i;
					}
				}
				curValue.set(tag, minDist);

				if (context.getSuperstepCounter() > 1) {
					if (!curValue.equals(lastValue)) {
						point.setVerValue(curValue);
						aggregators[curValue.getTag()].add(point.getDimensions(), 1);
						AggregatorOfKmeans aggregator = aggregators[lastValue.getTag()];
						aggregator.decrease(point.getDimensions(), 1);
						context.setVertexAgg((curValue.getDistance() - lastValue.getDistance()));
					}
				} else {
					point.setVerValue(curValue);
					aggregators[curValue.getTag()].add(point.getDimensions(), 1);
					context.setVertexAgg(curValue.getDistance());
				}
			}
		};

		kernel.execute(Range.create(1));
		kernel.dispose();
    }

    private static double computeDistance(int dim, double[] point, double[] center) {
        double sum = 0.0;
        double dist;
        for (int i = 0; i < dim; i++) {
            dist = Math.abs(point[i]-center[i]);
            sum += dist * dist;
        }

        return Math.sqrt(sum);
    }
}

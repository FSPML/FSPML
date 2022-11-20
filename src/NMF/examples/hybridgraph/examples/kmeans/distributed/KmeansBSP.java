/**
 * copyright 2011-2016
 */
package hybridgraph.examples.kmeans.distributed;

import com.aparapi.Kernel;
import com.aparapi.Range;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.io.EdgeParser;
import org.apache.hama.myhama.util.AggregatorOfKmeans;
import org.apache.hama.myhama.util.Context;
import org.apache.hama.myhama.util.KmeansValueTwo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;

/**
 * KmeansBSP.java implements {@link BSP}.
 *
 * Traditional Kmeans implementation.
 *
 * @author
 * @version 0.1
 */
public class KmeansBSP extends BSP<KmeansValue, Integer, Integer, Integer> {
	private static final Log LOG = LogFactory.getLog(KmeansBSP.class);

	private int numOfCentroids, numOfDimensions;
	private GraphRecord<KmeansValue, Integer, Integer, Integer>[] centroids;
	private Kernel kernel;
	private int startIdx;
	private int endIdx;
	// GPU暂时计算的数据
	private GraphRecord<KmeansValue, Integer, Integer, Integer>[] groupPoints;

	/**
	 * master get local data
	 */
	@Override
	public void getLocalData(GraphRecord<KmeansValue, Integer, Integer, Integer>[][] localPoints) {
		long time = System.currentTimeMillis();
		String path = "/home/daipanchao/kmeans_data/MASS_master_local";
		BufferedReader bufferedReader = null;
		String line;
		EdgeParser edgeParser = new EdgeParser();

		try {
			bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		int id = 2800000;
		try {
			while ((line = bufferedReader.readLine()) != null) {
				GraphRecord graphRecord = new GraphRecord();
				graphRecord.setVerId(id);
				KmeansValue kmeansValue = new KmeansValue();
				kmeansValue.set(id, Double.MAX_VALUE);
				graphRecord.setVerValue(kmeansValue);
				graphRecord.setDimensions(edgeParser.parseDimensionArray(line, ','));
				graphRecord.setIsSend(false);
				localPoints[0][id - 2800000] = graphRecord;
				id++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOG.info("getLocalData time is : " + (System.currentTimeMillis() - time));
	}

	@Override
	public void taskSetup(Context<KmeansValue, Integer, Integer, Integer> context) {
		this.numOfCentroids = context.getBSPJobInfo().getNumOfCenters();
		this.numOfDimensions = context.getBSPJobInfo().getNumOfDimensions();
		this.kernel = context.getKernel();
	}

	@Override
	public void superstepSetup(Context<KmeansValue, Integer, Integer, Integer> context) {
		this.centroids = context.getCentroids();
	}

	@Override
	public void update(Context<KmeansValue, Integer, Integer, Integer> context) throws Exception {
		GraphRecord<KmeansValue, Integer, Integer, Integer> point = context.getGraphRecord();
		AggregatorOfKmeans[] aggregators = context.getAggregators();

		KmeansValue lastValue = point.getVerValue();
		KmeansValue curValue = new KmeansValue();
		int tag = -1;
		double minDist = Double.MAX_VALUE;

		for (int i = 0; i < this.numOfCentroids; i++) {
			double dist = computeDistance(this.numOfDimensions,
					point.getDimensions(),
					this.centroids[i].getDimensions());

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

	@Override
	public void updateOnGpu(Context<KmeansValue, Integer, Integer, Integer> context) throws Exception {
		this.startIdx = context.getStartIdx();
		this.endIdx = context.getEndIdx();
		this.groupPoints = context.getGroupPoints();
		int groupLen = this.endIdx - this.startIdx + 1;
		AggregatorOfKmeans[] aggregators = context.getAggregators();

		kernel = new Kernel() {
			@Override
			public void run() {
				long time = System.currentTimeMillis();
				int gid = getGlobalId();
				if (gid + startIdx > endIdx) return;

				int tag = -1;
				double minDist = Double.MAX_VALUE;
				KmeansValue lastValue = groupPoints[gid].getVerValue();
				KmeansValue curValue = new KmeansValue();

				for (int i = 0; i < numOfCentroids; i++) {
					double dist = computeDistance(numOfDimensions,
							groupPoints[gid].getDimensions(),
							centroids[i].getDimensions());

					if (dist < minDist) {
						minDist = dist;
						tag = i;
					}
				}
				curValue.set(tag, minDist);

				if (context.getSuperstepCounter() > 1) {
					if (!curValue.equals(lastValue)) {
						groupPoints[gid].setVerValue(curValue);
						aggregators[curValue.getTag()].add(groupPoints[gid].getDimensions(), 1);
						AggregatorOfKmeans aggregator = aggregators[lastValue.getTag()];
						aggregator.decrease(groupPoints[gid].getDimensions(), 1);
						context.addVertexAgg((curValue.getDistance() - lastValue.getDistance()));
					}
				} else {
					groupPoints[gid].setVerValue(curValue);
					aggregators[curValue.getTag()].add(groupPoints[gid].getDimensions(), 1);
					context.addVertexAgg(curValue.getDistance());
				}
			}
		};

		kernel.execute(Range.create(groupLen));
	}

	private double computeDistance(int dim, double[] point, double[] center) {
		double sum = 0.0;
		double dist;

		for (int i = 0; i < dim; i++) {
			dist = Math.abs(point[i] - center[i]);
			sum += dist * dist;
		}

		return Math.sqrt(sum);
	}

}
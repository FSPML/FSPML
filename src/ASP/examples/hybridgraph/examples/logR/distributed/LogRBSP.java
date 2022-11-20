package hybridgraph.examples.logR.distributed;


import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.util.Context;
import org.apache.hama.myhama.util.GradientOfSGD;



public class LogRBSP extends BSP<LogRValue, Integer, Integer, Integer>{
	private int numOfDimensions;
	private double[] theta;
	
	@Override
	public void taskSetup(
			Context<LogRValue, Integer, Integer, Integer> context) {
		this.numOfDimensions = context.getBSPJobInfo().getNumOfDimensions();
		this.theta = new double[numOfDimensions-1];
		this.theta = context.getTheta();

	}
	
	@Override
	public void superstepSetup(
			Context<LogRValue, Integer, Integer, Integer> context) {
		this.theta = context.getTheta();
	}
	
	@Override
	public void update(Context<LogRValue, Integer, Integer, Integer> context) throws Exception {
		// TODO Auto-generated method stub
		GraphRecord<LogRValue, Integer, Integer, Integer> point = context.getGraphRecord();
		GradientOfSGD gradientAgg = context.getGradients();
		
		LogRValue lastValue = point.getVerValue();
		LogRValue curValue = new LogRValue();
		
		int startRepeatIte = context.getStartRepeatIte();
		int iteNum = context.getSuperstepCounter();
		
		double[] samples = new double[this.numOfDimensions]; //x1,x2,x3,……,xn, Y
		samples = point.getDimensions();
		double[] x = new double[numOfDimensions-1];
		double y = samples[numOfDimensions-1];

		double[] gradient = new double[theta.length];
		
		for (int i = 0; i < x.length; i++) {
			x[i] = samples[i];
		}
		
		double p = sigmoid(x);
		for (int i = 0; i < theta.length; i++) {
			gradient[i] =  (p - y) * x[i];
		}

		curValue.setLost(costFun(y, x));
		
		if(iteNum <= startRepeatIte+1) {
			point.setBaseVerValue(lastValue);
		}else if (iteNum == 3) {
			context.setVertexAgg(curValue.getLost());
		}else if(iteNum == startRepeatIte+2){
			lastValue = point.getBaseVerValue();
			context.setVertexAgg(curValue.getLost() - lastValue.getLost());
		}
		
		if(point.isFirstCompute()) {//第一遍计算
			point.setFirstCompute(false);
			point.setVerValue(curValue);
			gradientAgg.setGradient(gradient);
			context.setVertexAgg(curValue.getLost());
			
		}else { //非第一遍计算
			if (!curValue.equals(lastValue) && iteNum != startRepeatIte+2) {
				point.setVerValue(curValue);
				gradientAgg.setGradient(gradient);
				context.setVertexAgg((curValue.getLost()-lastValue.getLost()));
			}
			
		}
		
	}
    public double sigmoid(double[] x){ //得到P
        double z = 0;
        for (int i = 0; i < theta.length; i++) {
            z += theta[i] * x[i];
        }
        return 1.0 / (1.0 + Math.exp(-z));
    }

    public double costFun(double label, double[] x){
        return  -(label * Math.log(sigmoid(x)) + (1-label) * Math.log(1- sigmoid(x)));
    }

}

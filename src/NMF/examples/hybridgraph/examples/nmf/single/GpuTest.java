package hybridgraph.examples.nmf.single;

import com.aparapi.Kernel;
import com.aparapi.Range;

public class GpuTest {
    public static void main(String[] args) {
        final double[] a = new double[]{1.2658342, 6.325489};
        final double[] b = new double[]{5.278945645, 8.21536409};
        final double[] c = new double[2];
        final double[] d = new double[2];

        // cpu
//        for (int i = 0; i < 2; i++) {
//            c[i] = a[i] * b[i];
//            System.out.println("c[" + i + "] = " + c[i]);
//        }

        // gpu
        Kernel kernel = new Kernel() {
            @Override
            public void run() {
                int gid = getGlobalId();
                d[gid] = a[gid] * b[gid];
            }
        };

        kernel.execute(Range.create(2));

        for (int i = 0; i < 2; i++) {
            System.out.println("d[" + i + "] = " + d[i]);
        }

        kernel.dispose();
    }
}

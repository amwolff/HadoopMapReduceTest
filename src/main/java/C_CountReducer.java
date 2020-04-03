import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class C_CountReducer extends Reducer<IntWritable, ArrayPrimitiveWritable, IntWritable, ArrayPrimitiveWritable> {
    private final static IntWritable zero = new IntWritable(0);

    private DoubleWritable border1 = new DoubleWritable();
    private DoubleWritable border2 = new DoubleWritable();
    private DoubleWritable border3 = new DoubleWritable();
    private DoubleWritable maximum = new DoubleWritable();
    private IntWritable count = new IntWritable();
    private Writable[] tmpOut = new Writable[3];
    private ArrayPrimitiveWritable output = new ArrayPrimitiveWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        border1.set(configuration.getDouble("border1", 0));
        border2.set(configuration.getDouble("border2", 0));
        border3.set(configuration.getDouble("border3", 0));
        maximum.set(configuration.getDouble("maximum", 0));
    }

    @Override
    public void reduce(IntWritable key, Iterable<ArrayPrimitiveWritable> values, Context context) throws IOException, InterruptedException {
        int len = 0;
        for (ArrayPrimitiveWritable v : values) {
            len++;
        }
        count.set(len);

        tmpOut[2] = count;

        int k = key.get();
        if (k == 1) {
            tmpOut[0] = zero;
            tmpOut[1] = border1;
        } else if (k == 2) {
            tmpOut[0] = border1;
            tmpOut[1] = border2;
        } else if (k == 3) {
            tmpOut[0] = border2;
            tmpOut[1] = border3;
        } else if (k == 4) {
            tmpOut[0] = border3;
            tmpOut[1] = maximum;
        }

        output.set(tmpOut);

        context.write(key, output);
    }
}

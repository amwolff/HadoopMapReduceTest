import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class C_BordersMapper extends Mapper<Object, Text, IntWritable, ArrayPrimitiveWritable> {
    private Double b1;
    private Double b2;
    private Double b3;
    private IntWritable border = new IntWritable();
    private DoubleWritable tripID = new DoubleWritable();
    private DoubleWritable devRMS = new DoubleWritable();
    private DoubleWritable[] tmpOut = new DoubleWritable[2];
    private ArrayPrimitiveWritable output = new ArrayPrimitiveWritable();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        b1 = configuration.getDouble("b1", 0);
        b2 = configuration.getDouble("b2", 0);
        b3 = configuration.getDouble("b3", 0);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\s+");

        double primitiveTripID;
        double primitiveDevRMS;
        try {
            primitiveTripID = Double.parseDouble(values[0]);
            primitiveDevRMS = Double.parseDouble(values[1]);
        } catch (NumberFormatException e) {
            System.err.println(e.getMessage());
            return;
        }

        if (primitiveDevRMS > b3) {
            border.set(4);
        } else if (primitiveDevRMS > b2) {
            border.set(3);
        } else if (primitiveDevRMS > b1) {
            border.set(2);
        } else {
            border.set(1);
        }


        tripID.set(primitiveTripID);
        devRMS.set(primitiveDevRMS);
        tmpOut[0] = tripID;
        tmpOut[1] = devRMS;
        output.set(tmpOut);

        context.write(border, output);
    }
}

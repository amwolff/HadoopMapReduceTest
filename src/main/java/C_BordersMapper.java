import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class C_BordersMapper extends Mapper<Object, Text, IntWritable, ArrayPrimitiveWritable> {
    private Double border1;
    private Double border2;
    private Double border3;
    private IntWritable number = new IntWritable();
    private Text tripID = new Text();
    private DoubleWritable devRMS = new DoubleWritable();
    private Writable[] tmpOut = new Writable[2];
    private ArrayPrimitiveWritable output = new ArrayPrimitiveWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        border1 = configuration.getDouble("border1", 0);
        border2 = configuration.getDouble("border2", 0);
        border3 = configuration.getDouble("border3", 0);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\s+");

        tripID.set(values[0]);

        double primitiveDevRMS;
        try {
            primitiveDevRMS = Double.parseDouble(values[1]);
        } catch (NumberFormatException e) {
            System.err.println(e.getMessage());
            return;
        }

        if (primitiveDevRMS > border3) {
            number.set(4);
        } else if (primitiveDevRMS > border2) {
            number.set(3);
        } else if (primitiveDevRMS > border1) {
            number.set(2);
        } else {
            number.set(1);
        }

        devRMS.set(primitiveDevRMS);

        tmpOut[0] = tripID;
        tmpOut[1] = devRMS;
        output.set(tmpOut);

        context.write(number, output);
    }
}

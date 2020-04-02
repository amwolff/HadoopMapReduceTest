import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class A_TripsMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text tripID = new Text();
    private DoubleWritable deviation = new DoubleWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");

        if (values[7].equals("id_kursu")) {
            return;
        }

        tripID.set(values[7]);

        try {
            deviation.set(Double.parseDouble(values[15]));
        } catch (NumberFormatException e) {
            System.err.println(e.getMessage());
            return;
        }

        context.write(tripID, deviation);
    }
}

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class B_RMSMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private final static Text one = new Text("1");

    private DoubleWritable rms = new DoubleWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = GreatUtils.splitByWhitespace(value.toString());

        try {
            rms.set(Double.parseDouble(values[1]));
        } catch (NumberFormatException e) {
            System.err.println(e.getMessage());
            return;
        }

        context.write(one, rms);
    }
}

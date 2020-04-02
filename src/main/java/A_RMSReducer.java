import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class A_RMSReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        double len = 0;
        for (DoubleWritable v : values) {
            sum += Math.pow(v.get(), 2);
            len++;
        }
        result.set(Math.sqrt(sum / len));
        context.write(key, result);
    }
}

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class B_BordersReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private final static Text one = new Text("1");

    private DoubleWritable out = new DoubleWritable();

    private static int medianIndex(int n) {
        if (n % 2 == 0) {
            return n / 2;
        }
        return (n / 2) + 1;
    }

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,
            InterruptedException {

        ArrayList<Double> valuesList = new ArrayList<>();

        values.forEach(v -> valuesList.add(v.get()));

        Collections.sort(valuesList);

        int size = valuesList.size();
        int idx2 = medianIndex(size);
        int idx1 = medianIndex(idx2);
        int idx3 = medianIndex(size - idx2) + idx2;

        out.set(valuesList.get(idx1)); // Border 1
        context.write(one, out);
        out.set(valuesList.get(idx2)); // Border 2
        context.write(one, out);
        out.set(valuesList.get(idx3)); // Border 3
        context.write(one, out);
        out.set(valuesList.get(size - 1)); // Maximum
        context.write(one, out);
    }
}

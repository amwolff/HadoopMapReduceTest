import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class B_BordersReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    private static int medianIndex(int n) {
        if (n % 2 == 0) {
            return n / 2;
        }
        return (n / 2) + 1;
    }

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        ArrayList<Double> valuesList = new ArrayList<>();

        values.forEach(v -> valuesList.add(v.get()));

        Collections.sort(valuesList);

        int size = valuesList.size();
        int idx2 = medianIndex(size);
        int idx1 = medianIndex(idx2);
        int idx3 = medianIndex(size - idx2) + idx2;

        Configuration configuration = context.getConfiguration();
        configuration.setDouble("border1", valuesList.get(idx1));
        configuration.setDouble("border2", valuesList.get(idx2));
        configuration.setDouble("border3", valuesList.get(idx3));
        configuration.setDouble("maximum", valuesList.get(size - 1));
    }
}

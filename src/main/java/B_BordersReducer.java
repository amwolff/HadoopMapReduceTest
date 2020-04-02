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

        int i2 = medianIndex(valuesList.size());
        int i1 = medianIndex(i2);
        int i3 = medianIndex(valuesList.size() - i2) + i2;
        Double q1 = valuesList.get(i1);
        Double q2 = valuesList.get(i2);
        Double q3 = valuesList.get(i3);

        Configuration configuration = context.getConfiguration();
        configuration.setDouble("q1", q1);
        configuration.setDouble("q2", q2);
        configuration.setDouble("q3", q3);
    }
}

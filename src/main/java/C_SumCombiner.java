import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class C_SumCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable output = new IntWritable();

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {

        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }

        output.set(sum);

        context.write(key, output);
    }
}

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class C_CountReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, Text> {
    private final static String outputFormat = "lower: %f upper: %f length: %d";

    private Double border1;
    private Double border2;
    private Double border3;
    private Double maximum;
    private Text output = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] uris = context.getCacheFiles();
        if (!(uris != null && uris.length > 0)) {
            System.err.println("uris == null || uris.length == 0");
            return;
        }

        Path path = null;
        try {
            path = new Path(uris[0].toString());
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }

        BufferedReader reader =
                new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(path)));
        try {
            border1 = GreatUtils.parseBorder(reader.readLine());
            border2 = GreatUtils.parseBorder(reader.readLine());
            border3 = GreatUtils.parseBorder(reader.readLine());
            maximum = GreatUtils.parseBorder(reader.readLine());
        } catch (IOException | NumberFormatException e) {
            System.err.println(e.getMessage());
        }
    }

    @Override
    public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException,
            InterruptedException {

        int len = 0;
        for (DoubleWritable v : values) {
            len++;
        }

        int k = key.get();
        if (k == 1) {
            output.set(String.format(outputFormat, 0., border1, len));
        } else if (k == 2) {
            output.set(String.format(outputFormat, border1, border2, len));
        } else if (k == 3) {
            output.set(String.format(outputFormat, border2, border3, len));
        } else if (k == 4) {
            output.set(String.format(outputFormat, border3, maximum, len));
        }

        context.write(key, output);
    }
}

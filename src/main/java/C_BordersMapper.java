import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class C_BordersMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
    private double border1;
    private double border2;
    private double border3;
    private IntWritable number = new IntWritable();
    private DoubleWritable rms = new DoubleWritable();

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
            border1 = Utils.parseBorder(reader.readLine());
            border2 = Utils.parseBorder(reader.readLine());
            border3 = Utils.parseBorder(reader.readLine());
        } catch (IOException | NumberFormatException e) {
            System.err.println(e.getMessage());
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\s+");

        double devRMS;
        try {
            devRMS = Double.parseDouble(values[1]);
        } catch (NumberFormatException e) {
            System.err.println(e.getMessage());
            return;
        }

        if (devRMS >= border3) {
            number.set(4);
        } else if (devRMS >= border2) {
            number.set(3);
        } else if (devRMS >= border1) {
            number.set(2);
        } else {
            number.set(1);
        }

        rms.set(devRMS);

        context.write(number, rms);
    }
}

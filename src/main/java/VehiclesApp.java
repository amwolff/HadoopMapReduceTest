import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public final class VehiclesApp extends Configured {
    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.err.println("Usage: VehiclesApp <in> <out>");
            System.exit(2);
        }

        Job job1 = Job.getInstance(conf1, "A_VehicleApp");
        job1.setJarByClass(A_TripsMapper.class);
        job1.setMapperClass(A_TripsMapper.class);
        job1.setCombinerClass(A_RMSReducer.class);
        job1.setReducerClass(A_RMSReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        Path in1 = new Path(remainingArgs[0]);
        Path out1 = new Path(remainingArgs[1], "out1");

        FileInputFormat.addInputPath(job1, in1);
        FileOutputFormat.setOutputPath(job1, out1);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "B_VehicleApp");
        job2.setJarByClass(B_RMSMapper.class);
        job2.setMapperClass(B_RMSMapper.class);
        job2.setCombinerClass(B_BordersReducer.class);
        job2.setReducerClass(B_BordersReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        Path out2 = new Path(remainingArgs[1], "out2");

        FileInputFormat.addInputPath(job2, out1);
        FileOutputFormat.setOutputPath(job2, out2);

        job2.setNumReduceTasks(1);

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        Configuration conf3 = new Configuration(conf2);

        Job job3 = Job.getInstance(conf3, "C_VehicleApp");
        job2.setJarByClass(C_BordersMapper.class);
        job2.setMapperClass(C_BordersMapper.class);
        job2.setCombinerClass(C_CountReducer.class);
        job2.setReducerClass(C_CountReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(ArrayPrimitiveWritable.class);

        Path out3 = new Path(remainingArgs[1], "out3");

        FileInputFormat.addInputPath(job3, out1);
        FileInputFormat.addInputPath(job3, out3);

        job3.setNumReduceTasks(4);

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}

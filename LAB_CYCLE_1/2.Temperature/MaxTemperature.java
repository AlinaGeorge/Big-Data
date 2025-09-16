import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

    public static class TempMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text year = new Text();
        private IntWritable temperature = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Each line: YYYY-MM-DD<TAB>Temperature
            String[] parts = value.toString().trim().split("\\s+");

            if (parts.length == 2) {
                String date = parts[0];
                String tempStr = parts[1];
                try {
                    String yearStr = date.substring(0, 4);
                    int temp = Integer.parseInt(tempStr.trim());

                    year.set(yearStr);
                    temperature.set(temp);

                    context.write(year, temperature);
                } catch (Exception e) {
                    // Skip malformed lines
                }
            }
        }
    }

    public static class MaxReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable maxTemp = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            for (IntWritable val : values) {
                max = Math.max(max, val.get());
            }
            maxTemp.set(max);
            context.write(key, maxTemp);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Temperature Per Year");

        job.setJarByClass(MaxTemperature.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(MaxReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

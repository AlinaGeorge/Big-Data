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

public class MatrixMultiply {

    public static class MatrixMapper extends Mapper<Object, Text, Text, Text> {
        private int m; // rows in A
        private int n; // cols in A = rows in B
        private int p; // cols in B

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            m = conf.getInt("m", 0);
            n = conf.getInt("n", 0);
            p = conf.getInt("p", 0);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(" ");
            if (parts.length != 4) return;  // Skip malformed lines

            String matrix = parts[0];   // "A" or "B"
            int i = Integer.parseInt(parts[1]);
            int j = Integer.parseInt(parts[2]);
            int v = Integer.parseInt(parts[3]);

            if (matrix.equals("A")) {
                // A[i][j] contributes to all (i,k)
                for (int k = 0; k < p; k++) {
                    context.write(new Text(i + "," + k), new Text("A," + j + "," + v));
                }
            } else {
                // B[j][k] contributes to all (i,k)
                for (int r = 0; r < m; r++) {
                    context.write(new Text(r + "," + j), new Text("B," + i + "," + v));
                }
            }
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            java.util.HashMap<Integer, Integer> aMap = new java.util.HashMap<>();
            java.util.HashMap<Integer, Integer> bMap = new java.util.HashMap<>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                String tag = parts[0];
                int index = Integer.parseInt(parts[1]);
                int v = Integer.parseInt(parts[2]);

                if (tag.equals("A")) {
                    aMap.put(index, v);
                } else {
                    bMap.put(index, v);
                }
            }

            int result = 0;
            for (int k : aMap.keySet()) {
                if (bMap.containsKey(k)) {
                    result += aMap.get(k) * bMap.get(k);
                }
            }
            context.write(key, new IntWritable(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        if (args.length < 5) {
            System.err.println("Usage: MatrixMultiply <input> <output> <m> <n> <p>");
            System.exit(2);
        }

        conf.setInt("m", Integer.parseInt(args[2]));
        conf.setInt("n", Integer.parseInt(args[3]));
        conf.setInt("p", Integer.parseInt(args[4]));

        Job job = Job.getInstance(conf, "Matrix Multiply");
        job.setJarByClass(MatrixMultiply.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

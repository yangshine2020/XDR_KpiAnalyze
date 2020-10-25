package mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataProcess {


    private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{


        private Text outkey = new Text();
        private IntWritable outval = new IntWritable();
        private String[] strs = null;

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {


            strs = value.toString().split("|");

            for (String s : strs){
                outkey.set(s);
                outval.set(1);

                context.write(outkey, outval);
            }
        }
    }




    private static class MyReducer extends Reducer<Text, IntWritable, Text, LongWritable>{


        private LongWritable outval = new LongWritable();
        private Long sum = 0L;

        @Override
        protected void reduce(Text outkey, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {

            sum = 0L; //

            for (IntWritable i : values){
                sum+=i.get();
            }
            outval.set(sum);

            context.write(outkey, outval);
        }
    }



    public static void main(String[] args) {




        Configuration conf = new Configuration();
        try {

            Job job = Job.getInstance(conf, "mywc");


            job.setJarByClass(DataProcess.class);
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            Path in = new Path(args[0]);
            Path out = new Path(args[1]);
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(out)){
                // hadoop fs -rm -r skipTrash
                fs.delete(out, true);
                System.out.println(job.getJobName() + "'s old path output id deleted!");
            }
            FileInputFormat.addInputPath(job, in);
            FileOutputFormat.setOutputPath(job, out);


            boolean con = job.waitForCompletion(true);
            String msg = con?"JOB_STATUS : OK!":"JOB_STATUS : FAIL!";
            System.out.println(msg);


        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
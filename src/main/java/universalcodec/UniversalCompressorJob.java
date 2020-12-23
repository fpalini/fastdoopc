package universalcodec;

import java.io.IOException;
import java.util.Random;

import fastdoop.Record;
import fastdoop.compression.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This job allows to compress the input file using the {@code UniversalCodec} class.
 *
 * @author Francesco Palini
 * @see UniversalCodec
 */
public class UniversalCompressorJob {

    public int compress(String[] args, Configuration conf) throws Exception {
        conf.set("io.compression.codecs", "universalcodec.UniversalCodec");

        String input = args[0];
        String output = args[1];
        String codec = args[2];
        String seqtype = args[3];

        conf.set("universalcodec.codec", codec);

        conf.setBoolean("universalcodec.assembled", true);

        String job_name = "UC Compression with " + codec + " on " + input;
        Job job = Job.getInstance(conf, job_name);
        FileSystem fs = FileSystem.get(conf);

        Path input_path = new Path(input);
        Path output_path = new Path(output);

        fs.delete(output_path, true);

        job.setJarByClass(this.getClass());

        job.setMapperClass(CSMapper.class);
        job.setReducerClass(CSReducer.class);

        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        if (seqtype.equals("fasta"))
            job.setInputFormatClass(FASTAshortInputFileFormat.class);
        else // FASTQ
            job.setInputFormatClass(FASTQInputFileFormat.class);

        FileInputFormat.addInputPath(job, input_path);
        FileOutputFormat.setOutputPath(job, output_path);

        FileOutputFormat.setOutputCompressorClass(job, UniversalCodec.class);  // uncomment to check output with input

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class CSMapper extends Mapper<Text, Record, LongWritable, Text> {

        public void map(Text no_value, Record record, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(new Random().nextLong()), new Text(record.toString()));
        }
    }

    public static class CSReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

        public void reduce(LongWritable id, Iterable<Text> lines, Context context) throws IOException, InterruptedException {
            for (Text line : lines)
                context.write(line, NullWritable.get());
        }
    }
}
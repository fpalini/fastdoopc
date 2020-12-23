package benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import benchmark.task.CountMapReduce;
import benchmark.task.CountMap;
import fastdoop.compression.FASTAshortInputFileFormat;
import fastdoop.compression.FASTAshortMcInputFileFormat;
import fastdoop.compression.FASTAshortMzInputFileFormat;
import fastdoop.compression.FASTQDSRCInputFormat;
import fastdoop.compression.FASTQInputFileFormat;
import fastdoop.compression.FASTQMcInputFormat;
import fastdoop.compression.FASTQMzInputFormat;
import fastdoop.compression.FASTQUniversalInputFormat;
import fastdoop.compression.FASTAShortUniversalInputFormat;

public class BenchmarkJob {

    public static int bench(String input_path, String output_path, String task_type, String seq_type, Configuration conf) throws Exception {

        int reducers_args = 5; // only 5 keys: A, C, G, T, N

        // SET SEQUENCE TYPE
        String sequenceType;
        Class sequenceInputFormatClass;

        switch (seq_type) {

            case "fasta": {

                sequenceType = "FASTA";

                if (input_path.endsWith("4mc"))
                    sequenceInputFormatClass = FASTAshortMcInputFileFormat.class;

                else if (input_path.endsWith("4mz"))
                    sequenceInputFormatClass = FASTAshortMzInputFileFormat.class;

                else if (input_path.endsWith("uc"))
                    sequenceInputFormatClass = FASTAShortUniversalInputFormat.class;

                else // uncompressed or Hadoop-way compressed file
                    sequenceInputFormatClass = FASTAshortInputFileFormat.class;

                break;
            }

            case "fastq": {

                sequenceType = "FASTQ";

                if (input_path.endsWith("dsrc"))
                    sequenceInputFormatClass = FASTQDSRCInputFormat.class;

                else if (input_path.endsWith("4mc"))
                    sequenceInputFormatClass = FASTQMcInputFormat.class;

                else if (input_path.endsWith("4mz"))
                    sequenceInputFormatClass = FASTQMzInputFormat.class;

                else if (input_path.endsWith("uc"))
                    sequenceInputFormatClass = FASTQUniversalInputFormat.class;

                else // uncompressed or Hadoop-way compressed file
                    sequenceInputFormatClass = FASTQInputFileFormat.class;

                break;
            }

            default: {
                throw new IllegalArgumentException("Invalid Sequence Type");
            }
        }

        String mapType;
        Class mapClass;
        Class reduceClass = null;
        Class taskClass = null;

        switch (task_type) {

            case "map": {

                mapType = "Count Map";
                mapClass = CountMap.class;
                taskClass = CountMap.class;

                break;
            }

            case "mapreduce": {

                mapType = "Count MapReduce";
                mapClass = CountMapReduce.CountMapper.class;
                reduceClass = CountMapReduce.CountReducer.class;
                taskClass = CountMapReduce.class;

                break;
            }

            default: {

                mapType = null;
                mapClass = null;
            }

        }

        Path hdfsInputPath = new Path(input_path);
        Path hdfsOutputPath = new Path(output_path);

        String fileName = hdfsInputPath.getName();

        conf.set("io.compression.codecs",
                (conf.get("io.compression.codecs") == null? "" : conf.get("io.compression.codecs") + ",") +
                        "org.apache.hadoop.io.compress.BZip2Codec," +
                        "org.apache.hadoop.io.compress.GzipCodec," +
                        "org.apache.hadoop.io.compress.DefaultCodec," +

                        "com.hadoop.compression.fourmc.Lz4Codec," +
                        "com.hadoop.compression.fourmc.Lz4MediumCodec," +
                        "com.hadoop.compression.fourmc.Lz4HighCodec," +
                        "com.hadoop.compression.fourmc.Lz4UltraCodec," +

                        "com.hadoop.compression.fourmc.FourMcCodec," +
                        "com.hadoop.compression.fourmc.FourMcMediumCodec," +
                        "com.hadoop.compression.fourmc.FourMcHighCodec," +
                        "com.hadoop.compression.fourmc.FourMcUltraCodec," +

                        "com.hadoop.compression.fourmc.FourMzCodec," +
                        "com.hadoop.compression.fourmc.FourMzMediumCodec," +
                        "com.hadoop.compression.fourmc.FourMzHighCodec," +
                        "com.hadoop.compression.fourmc.FourMzUltraCodec, " +

                        "dsrc.DsrcCodec," +
                        "universalcodec.UniversalCodec");

        FileSystem fs = FileSystem.get(conf);
        fs.delete(hdfsOutputPath, true);

        String jobname = "Input: " + fileName + " - Sequence Type: " + sequenceType + " - Map Type: " + mapType;
        Job job = Job.getInstance(conf, jobname);

        if (reduceClass == null) {
            job.setNumReduceTasks(0);
            job.setMapOutputValueClass(Text.class);
            job.setOutputValueClass(Text.class);
        } else {
            job.setReducerClass(reduceClass);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputValueClass(LongWritable.class);

            job.setNumReduceTasks(reducers_args);
        }

        job.setJarByClass(taskClass);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);

        job.setMapperClass(mapClass);

        job.setInputFormatClass(sequenceInputFormatClass);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, hdfsInputPath);
        FileOutputFormat.setOutputPath(job, hdfsOutputPath);

        // LAUNCH

        int res = job.waitForCompletion(true) ? 0 : 1;

        return res;
    }

}

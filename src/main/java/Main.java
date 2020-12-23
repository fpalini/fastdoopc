import benchmark.BenchmarkJob;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import universalcodec.UniversalCompressorUtility;

import java.util.Iterator;

public class Main {
    public static void main(String[] args) throws Exception {
        String conf_file_path = args.length > 0 ? args[0] : "uc.conf";

        Configuration conf_file = new PropertiesConfiguration(conf_file_path);

        Iterator<String> iter = conf_file.getKeys();

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        String task = null;
        String input = null;
        String output = null;
        String bench = null;
        String seq_type = null;

        String key, value;
        while(iter.hasNext()) {
            key = iter.next();
            value = conf_file.getString(key);

            switch (key) {
                case "task":
                    task = value;
                    break;
                case "input":
                    input = value;
                    break;
                case "output":
                    output = value;
                    break;
                case "bench":
                    bench = value;
                    break;
                case "sequence.type":
                    seq_type = value;
                    break;
                default:
                    conf.set(key, value);
                    break;
            }
        }

        System.out.println("Configuration Loaded: " + conf_file_path);

        assert input != null;
        assert output != null;

        String local_out = input + "." + conf.get("uc.codec") + ".uc";

        if (task != null && task.equals("benchmark")) {
            assert bench != null;
            assert seq_type != null;
            BenchmarkJob.bench(input, output, bench, seq_type, conf);
        }
        else {
            UniversalCompressorUtility.compress(input, local_out, conf);
            FileSystem fs = FileSystem.get(conf);
            fs.copyFromLocalFile(new Path(local_out), new Path(output));
        }
    }
}

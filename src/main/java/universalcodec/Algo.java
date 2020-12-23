package universalcodec;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

public class Algo {
    public final String name;
    public final String compress_cmd;
    public final String decompress_cmd;
    public final String decompress_ext;
    public final String compress_ext;
    public final String io_input_flag;
    public final String io_output_flag;
    public final boolean io_reverse;
    public final boolean io_output_explicit;

    public Algo(String name, String compress_cmd, String decompress_cmd, String compress_ext, String decompress_ext,
                    String io_input_flag, String io_output_flag, boolean io_reverse, boolean io_output_explicit) {
        this.name = name;
        this.compress_cmd = compress_cmd;
        this.decompress_cmd = decompress_cmd;
        this.compress_ext = compress_ext;
        this.decompress_ext = decompress_ext;
        this.io_input_flag = io_input_flag;
        this.io_output_flag = io_output_flag;
        this.io_reverse = io_reverse;
        this.io_output_explicit = io_output_explicit;
    }

    public static Algo create(Configuration conf) {
        String codec = conf.get("uc.codec").toUpperCase();
        conf.set("uc.codec", codec);

        String compress_cmd = conf.get("uc."+codec+".compress.cmd");
        String decompress_cmd = conf.get("uc."+codec+".decompress.cmd");
        String compress_ext = conf.get("uc."+codec+".compress.ext");
        String decompress_ext = conf.get("uc."+codec+".decompress.ext", "fastq");
        String io_input_flag = conf.get("uc."+codec+".io.input.flag", "");
        String io_output_flag = conf.get("uc."+codec+".io.output.flag", "");
        boolean io_reverse = conf.getBoolean("uc."+codec+".io.reverse", false);
        boolean io_output_explicit = conf.getBoolean("uc."+codec+".io.output.explicit", true);

        return new Algo(codec, compress_cmd, decompress_cmd, compress_ext, decompress_ext,
                            io_input_flag, io_output_flag, io_reverse, io_output_explicit);
    }

    public static Algo create(byte[] md5, Configuration conf) {
        for (Map.Entry<String, String> entry : conf)
            if (entry.getKey().contains("compress.cmd")) {
                String codec = entry.getKey().split("\\.")[1].toUpperCase();
                if (Arrays.equals(DigestUtils.md5(codec), md5)) {
                    conf.set("uc.codec", codec);
                    return create(conf);
                }
            }

        return null;
    }

    public byte[] compress(byte[] data) {
        return execute(data, true);
    }

    public byte[] decompress(byte[] data) {
        return execute(data, false);
    }

    private byte[] execute(byte[] data, boolean compress) {
        try {
            if (data.length == 0)
                return null;

            String cmd, in_ext, out_ext;

            if (compress) {
                cmd = compress_cmd;
                in_ext = decompress_ext;
                out_ext = compress_ext;
            }
            else {
                cmd = decompress_cmd;
                in_ext = compress_ext;
                out_ext = decompress_ext;
            }

            int id = (int) (Math.random() * 1e6);

            String in_path = "/dev/shm/uc_tmp" + id + "." + in_ext;
            String out_path = "/dev/shm/uc_tmp" + id + "." + out_ext;

            Files.write(Paths.get(in_path), data);

            String in_flag = io_input_flag;
            in_flag += in_flag.equals("") ? "" : " ";

            String out_flag = io_output_flag;
            out_flag += out_flag.equals("") ? "" : " ";

            if (io_reverse && io_output_explicit)
                cmd += " " + out_flag + out_path + " " + in_flag + in_path;
            else if (!io_reverse && io_output_explicit)
                cmd += " " + in_flag + in_path + " " + out_flag + out_path;
            else // !explicit_output
                cmd += " " + in_flag + in_path;

            ProcessBuilder pb = new ProcessBuilder(cmd.split(" "));

            Process p = pb.start();

            // OutputStream out = p.getOutputStream();
            // InputStream in = p.getInputStream();

			/*if (!ramdisk) {
				out.write(data);
				out.flush();
				out.close();
			}*/

            // System.out.println("CMD: " + cmd);

            p.waitFor();

            System.err.println(new String(IOUtils.toByteArray(p.getErrorStream())));

            byte[] out_data;

            //if (ramdisk) {
            out_data = Files.readAllBytes(Paths.get(out_path));
            Files.delete(Paths.get(in_path));
            Files.delete(Paths.get(out_path));
			/*}
			else
				out_data = IOUtils.toByteArray(in);*/

            // System.out.println("IN: "+data.length+"   OUT: "+out_data.length);

            return out_data;

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        return null;
    }
}

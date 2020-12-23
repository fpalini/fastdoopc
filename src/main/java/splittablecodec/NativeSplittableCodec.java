package splittablecodec;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.*;


public abstract class NativeSplittableCodec implements SplittableCompressionCodec, Configurable {
	
	private static boolean libraryLoaded = false;
	private Configuration conf;

	static {
	        String packagePrefix = NativeSplittableCodec.class.getPackage().getName().replace('.', '/');
	        InputStream is = NativeSplittableCodec.class.getResourceAsStream("/"+packagePrefix+"/libcodec.so");
	        if (is == null) {
	            throw new UnsupportedOperationException("Resource /"+packagePrefix+"/libcodec.so not found");
	        }
	        File tempLib;
	        try {
	            tempLib = File.createTempFile("libcodec", ".so");

	            FileOutputStream out = new FileOutputStream(tempLib);
	            try {
	                byte[] buf = new byte[4096];
	                while (true) {
	                    int read = is.read(buf);
	                    if (read == -1) {
	                        break;
	                    }
	                    out.write(buf, 0, read);
	                }
	                try {
	                    out.close();
	                    out = null;
	                } catch (IOException e) {
	                }
	                System.load(tempLib.getAbsolutePath());
	                libraryLoaded = true;
	                System.err.println("LIBCODEC LOADED");
	            } finally {
	                try {
	                    if (out != null) {
	                        out.close();
	                    }
	                } catch (IOException e) {
	                }
	                if (tempLib.exists()) {
	                    if (!libraryLoaded) {
	                        tempLib.delete();
	                    } else {
	                        tempLib.deleteOnExit();
	                    }
	                }
	            }
	        } catch (Exception e) {
	            System.err.println("LIBCODEC NOT LOADED");
	        }
	}

	@Override
	public Class<? extends Decompressor> getDecompressorType() {
		return NativeCodecDecompressor.class;
	}

	@Override
	public Decompressor createDecompressor() {
		return new NativeCodecDecompressor(conf);
	}

	public CompressionInputStream createInputStream(InputStream in) throws IOException {
		return createInputStream(in, createDecompressor());
	}

	public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
		return new BlockDecompressorStream(in, decompressor);
	}

	public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
		return createOutputStream(out, createCompressor());
	}

	public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
		return new BlockCompressorStream(out, compressor);
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public SplitCompressionInputStream createInputStream(InputStream inputStream, Decompressor decompressor, long start, long end, READ_MODE read_mode) throws IOException {
		return new CodecInputStream(inputStream, decompressor, start, end, conf);
	}
}

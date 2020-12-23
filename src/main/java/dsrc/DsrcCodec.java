package dsrc;

import splittablecodec.NativeSplittableCodec;
import org.apache.hadoop.io.compress.*;

public class DsrcCodec extends NativeSplittableCodec {

	public Compressor createCompressor() {
		throw new RuntimeException("DSRC actually not supported as a compressor");
	}

	public Class<? extends Compressor> getCompressorType() {
		throw new RuntimeException("DSRC actually not supported as a compressor");
	}

	public String getDefaultExtension() {
		return ".dsrc";
	}
}

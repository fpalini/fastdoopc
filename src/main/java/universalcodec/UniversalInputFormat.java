package universalcodec;
import java.io.IOException;

import splittablecodec.CodecInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public abstract class UniversalInputFormat<K, V> extends CodecInputFormat<K, V> {

	@Override
	public int getStartData() {
		return UniversalCodec.HEADER_SIZE;
	}

	@Override
	public void extractCodecInfo(FSDataInputStream archive, long fileLen, Configuration conf) throws IOException {
		int header_dummy = archive.readByte();
		byte[] md5 = new byte[16];
		archive.readFully(1, md5);

		Algo.create(md5, conf);
		
		if (header_dummy != -95) throw new IOException("Wrong header dummy value: "+header_dummy+" != " + -95); // 0xA1
		
		archive.seek(fileLen - 4 - 8);
		
		int nblocks = Ints.fromBytes(archive.readByte(), archive.readByte(), archive.readByte(), archive.readByte());
		long footer_offset = Longs.fromBytes(archive.readByte(), archive.readByte(), archive.readByte(), archive.readByte(), 
												archive.readByte(), archive.readByte(), archive.readByte(), archive.readByte());
		
		byte[] footer = new byte[4*nblocks+2];
		archive.read(footer_offset, footer, 0, 4*nblocks+2);
		
		if (footer[0] != -94) throw new IOException("Wrong start footer dummy value: "+footer[0]+" != " + -94); // 0xA2
		if (footer[4*nblocks+1] != -1) throw new IOException("Wrong end footer dummy value: "+footer[4*nblocks+1]+" != " + -1); // 0xFF
		
		extractBlockSizes(footer, 1, nblocks);
	}
}

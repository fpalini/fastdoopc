package universalcodec;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

import static universalcodec.UniversalCodec.UNCMP_BLOCK_SIZE;
import static universalcodec.UniversalCodec.HEADER_SIZE;

public class UniversalCompressorUtility {

	public static void compress(String input, String output, Configuration conf) throws IOException {
		// compress <input> <codec>
		Algo algo = Algo.create(conf);

		byte delimiter = delimiter(algo);

		String comp_file = output;

		FileInputStream istream = new FileInputStream(input);
		FileOutputStream ostream = new FileOutputStream(comp_file);
		byte[] buffer = new byte[UniversalCodec.CMP_BUFFER_SIZE];

		int off = 0;

		ostream.write(0xA1);
		ostream.write(DigestUtils.md5(algo.name));

		ArrayList<Integer> blocksizes = new ArrayList<>();
		long total = 0;

		while(istream.available() > 0) {
			int n = istream.read(buffer, off, UNCMP_BLOCK_SIZE-off);
			int m = n;

			if (off == 0)
				off++;
			else
				m += off;

			if (m == UNCMP_BLOCK_SIZE)
				while(istream.available() > 0) {
					byte nextByte = (byte) istream.read();

					if (nextByte == delimiter && buffer[m-1] == '\n') {
						// FASTQ check
						if (delimiter == '@') {
							int n_prevlines = 0;

							for (int i = m - 2; i > 0; i--) {
								if (buffer[i] == '\n')
									n_prevlines++;
								else if (buffer[i] == '+')
									break;
							}

							if (n_prevlines == 0) {// quality line
								buffer[m] = nextByte;
								m += 1;
							} else // new read
								break;
						}
						else
							break;
					}
					else {
						buffer[m] = nextByte;
						m += 1;
					}
				}

			byte[] out_data = algo.compress(Arrays.copyOf(buffer, m));

			assert out_data != null;
			blocksizes.add(out_data.length);
			ostream.write(out_data);

			buffer[0] = delimiter;

			total += m;
            System.out.printf("Compressed: %.1fGB\n", (total * 1.0 / (1024 * 1024 * 1024)));
		}

		ostream.write(0xA2);

		for (int bs : blocksizes)
			ostream.write(Ints.toByteArray(bs));

		long tot_sizes = 0;

		for (int bs : blocksizes)
			tot_sizes += bs;

		ostream.write(0xFF);

		ostream.write(Ints.toByteArray(blocksizes.size()));
		ostream.write(Longs.toByteArray(HEADER_SIZE + tot_sizes));

		istream.close();
		ostream.close();
	}

	public static byte delimiter(Algo algo) {
		if (algo.decompress_ext.equals("fasta") || algo.decompress_ext.equals("fa"))
			return '>';
		else if (algo.decompress_ext.equals("fastq") || algo.decompress_ext.equals("fq"))
			return '@';

		return '\0';
	}
}

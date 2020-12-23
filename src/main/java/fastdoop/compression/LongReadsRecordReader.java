/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fastdoop.compression;

import java.io.IOException;
import java.util.ArrayList;

import fastdoop.PartialSequence;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

import org.apache.hadoop.mapreduce.RecordReader;

/**
 * This class reads {@literal <key, value>} pairs from an {@code InputSplit}.
 * The input file is in FASTA format and contains a single long sequence.
 * A FASTA record has a header line that is the key, and data lines
 * that are the value.
 * {@literal >header...}
 * data
 * ...
 * 
 * 
 * Example:
 * {@literal >Seq1}
 * TAATCCCAAATGATTATATCCTTCTCCGATCGCTAGCTATACCTTCCAGGCGATGAACTTAGACGGAATCCACTTTGCTA
 * CAACGCGATGACTCAACCGCCATGGTGGTACTAGTCGCGGAAAAGAAAGAGTAAACGCCAACGGGCTAGACACACTAATC
 * CTCCGTCCCCAACAGGTATGATACCGTTGGCTTCACTTCTACTACATTCGTAATCTCTTTGTCAGTCCTCCCGTACGTTG
 * GCAAAGGTTCACTGGAAAAATTGCCGACGCACAGGTGCCGGGCCGTGAATAGGGCCAGATGAACAAGGAAATAATCACCA
 * CCGAGGTGTGACATGCCCTCTCGGGCAACCACTCTTCCTCATACCCCCTCTGGGCTAACTCGGAGCAAAGAACTTGGTAA
 * ...
 * 
 * @author Gianluca Roscigno
 * 
 * @version 1.0
 * 
 * @see InputSplit
 */
public class LongReadsRecordReader extends RecordReader<Text, PartialSequence> {

	final int BLOCK_BUFFER_SIZE = 2000000;

	private FSDataInputStream inputFile;
	
	private SplitCompressionInputStream cInputFile;

	private long startByte, endByte;

	private Text currKey;

	private PartialSequence currValue;

	/*
	 * True, if we processed the entire input split buffer. False, otherwise
	 */
	private boolean endMyInputSplit;

	private int k;
	
	public LongReadsRecordReader() {
		super();
		
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {

		Configuration job = context.getConfiguration();
		
		/*
		 * k determines how many bytes of the next input split (if any) 
		 * should be retrieved together with the bytes of the current
		 * input split.  
		 */
		k = context.getConfiguration().getInt("k", 10);

		/*
		 * We open the file corresponding to the input split and
		 * start processing it
		 */
		FileSplit split = (FileSplit) genericSplit;
		Path path = split.getPath();
		startByte = split.getStart();
		endByte = startByte + split.getLength();
		inputFile = path.getFileSystem(job).open(path);

		currKey = new Text("null");
		currValue = new PartialSequence();

		/*
		 * We read the whole content of the split in memory using
		 * myInputSplitBuffer. Plus, we read in the memory the first
		 * k+2 characters of the next split
		 */

		int inputSplitSize = (int) split.getLength();
		int otherbytesToReads = k + 2;
		
		byte[] myInputSplitBuffer = null;
		int sizeBuffer1 = 0;
		
		CompressionCodec codec = new CompressionCodecFactory(job).getCodec(path);
		
		if (codec == null) {// uncompressed file
			myInputSplitBuffer = new byte[(inputSplitSize + otherbytesToReads)];
			sizeBuffer1 = inputFile.read(startByte, myInputSplitBuffer, 0, inputSplitSize);
		}
		else if (codec instanceof SplittableCompressionCodec) { // compressed with BZ2
			myInputSplitBuffer = readCompressedSplit((SplittableCompressionCodec) codec);
			sizeBuffer1 = myInputSplitBuffer.length - otherbytesToReads;
		}
		else try { throw new Exception("Codec not supported."); } 
		catch(Exception e) {}

		currValue.setBuffer(myInputSplitBuffer);

		if (sizeBuffer1 <= 0) {
			endMyInputSplit = true;
			return;
		} else
			endMyInputSplit = false;

		int sizeBuffer2 = 0;
		
		if (codec == null) {// uncompressed file
			sizeBuffer2 = inputFile.read((startByte + sizeBuffer1), myInputSplitBuffer, sizeBuffer1, otherbytesToReads);
}
		else if (codec instanceof SplittableCompressionCodec) { // compressed with BZ2
			sizeBuffer2 =  cInputFile.read(myInputSplitBuffer, sizeBuffer1, otherbytesToReads);}
		else try { throw new Exception("Codec not supported."); } 
		catch(Exception e) {}
		
		boolean lastInputSplit = false;

		/*
		 * If there are no characters to read from the next split, then
		 * this is the last split
		 */
		if (sizeBuffer2 <= 0) {
			lastInputSplit = true;
			sizeBuffer2 = 0;
		}

		int posBuffer = 0;

		/*
		 * If we are processing the first split of the HDFS file, then we need
		 * to discard the comment line
		 */
		if (startByte == 0) {

			for (int i = 0; i < sizeBuffer1; i++) {

				posBuffer++;

				if (myInputSplitBuffer[posBuffer - 1] == '\n')
					break;

			}

		}

		/*
		 * If the split we are processing is not the last one, then we need
		 * to process its whole content
		 */
		if (!lastInputSplit) {
			currValue.setBytesToProcess(sizeBuffer1 - posBuffer);

			if (sizeBuffer2 < (k - 1)) {
				currValue.setBytesToProcess(currValue.getBytesToProcess() - ((k - 1) - sizeBuffer2));
			}

		} else {
			/*
			 * If the split we are processing is the last one, we trim
			 * all the ending '\n' characters
			 */

			int c = 0;

			for (int i = sizeBuffer1 - 1; i >= 0; i--) {
				if (myInputSplitBuffer[i] != '\n')
					break;

				c++;
			}

			currValue.setBytesToProcess((sizeBuffer1 - posBuffer) - k + 1 - c);
			if (currValue.getBytesToProcess() <= 0) {
				endMyInputSplit = true;
			}

		}

		currValue.setHeader(path.getName());
		currValue.setStartValue(posBuffer);
		currValue.setEndValue(sizeBuffer1 + sizeBuffer2 - 1);

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (endMyInputSplit == true)
			return false;

		endMyInputSplit = true;
		return true;

	}

	@Override
	public void close() throws IOException {// Close the record reader.
		if (inputFile != null)
			inputFile.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return currKey;
	}

	@Override
	public PartialSequence getCurrentValue() throws IOException, InterruptedException {
		return currValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return endMyInputSplit ? 1 : 0;
	}

	private byte[] readCompressedSplit(SplittableCompressionCodec codec){
		try {
			cInputFile = codec.createInputStream(inputFile, codec.createDecompressor(), startByte, endByte, SplittableCompressionCodec.READ_MODE.BYBLOCK);
			startByte = cInputFile.getAdjustedStart(); 
			endByte = cInputFile.getAdjustedEnd();
			
			ArrayList<BlockBuffer> blocks = new ArrayList<BlockBuffer>();
			int totalSize = 0;
			int readByte= 0;
			
			byte[] inputSplitBuffer, blockBuff;
			
			while(cInputFile.getPos() < endByte){

				blockBuff = new byte[BLOCK_BUFFER_SIZE];
				readByte = cInputFile.read(blockBuff, 0, BLOCK_BUFFER_SIZE);

				if(readByte > 0){
					totalSize += readByte;
					blocks.add(new BlockBuffer(blockBuff, readByte));
				}
			}
			
			int otherbytesToReads = k + 2;
			inputSplitBuffer = new byte[totalSize + otherbytesToReads];

			int destPos = 0;

			for(int i=0; i<blocks.size(); i++){
				BlockBuffer b = blocks.get(i);
				System.arraycopy(b.getBuffer(), 0, inputSplitBuffer, destPos, b.getLenght());
				destPos += b.getLenght();
			}

			return inputSplitBuffer;

		} catch (IOException e) {
			return new byte[0];
		} 
	}
	
	private static class BlockBuffer {
		private byte[] buffer;
		private int lenght;
		
		public BlockBuffer(byte[] b, int l){
			this.buffer=b;
			this.lenght=l;
		}

		public byte[] getBuffer() {
			return buffer;
		}

		public int getLenght() {
			return lenght;
		}
	}
}
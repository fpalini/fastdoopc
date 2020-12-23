#include "../include/dsrc/splittablecodec_NativeCodecDecompressor.h"

#include "../include/dsrc/Globals.h"
#include "BitMemory.h"
#include "BlockCompressor.h"
#include "DsrcIo.h"
#include <cstring>
#include <iostream>

using namespace dsrc;
using namespace dsrc::comp;
using namespace dsrc::fq;
using namespace dsrc::core;

static jfieldID NativeCodecDecompressor_compressedDirectBuf;
static jfieldID NativeCodecDecompressor_uncompressedDirectBuf;
static jfieldID NativeCodecDecompressor_compressedDirectBufLen;
static jfieldID NativeCodecDecompressor_compressedDirectBufOff;

byte* compressed_bytes;
byte* uncompressed_bytes;

static FastqDatasetType datasetType;
static CompressionSettings comprSettings;

FastqDataChunk* fastqChunk;

JNIEXPORT void JNICALL Java_splittablecodec_NativeCodecDecompressor_initIDs
  (JNIEnv* env, jclass cls) {

	NativeCodecDecompressor_compressedDirectBuf = env->GetFieldID(cls,"compressedDirectBuf", "Ljava/nio/ByteBuffer;");
	NativeCodecDecompressor_uncompressedDirectBuf = env->GetFieldID(cls,"uncompressedDirectBuf", "Ljava/nio/ByteBuffer;");
	NativeCodecDecompressor_compressedDirectBufLen = env->GetFieldID(cls, "compressedDirectBufLen", "I");
	NativeCodecDecompressor_compressedDirectBufOff = env->GetFieldID(cls, "compressedDirectBufOff", "I");

	fastqChunk = new FastqDataChunk(FastqDataChunk::DefaultBufferSize);
}


JNIEXPORT void JNICALL Java_splittablecodec_NativeCodecDecompressor_setParameters
  (JNIEnv * env, jclass cls, jbyteArray parameters_array) {

    jbyte* parameters = env->GetByteArrayElements(parameters_array, NULL);

    byte datasetTypeFlags = parameters[0];
    byte qualityOffset = parameters[1];
    byte compressionInfoFlags = parameters[2];
    byte dnaOrder = parameters[3];
    byte qualityOrder = parameters[4];

	datasetType.colorSpace = (datasetTypeFlags & DsrcFileFooter::FLAG_COLOR_SPACE) != 0;
	datasetType.plusRepetition = (datasetTypeFlags & DsrcFileFooter::FLAG_PLUS_REPETITION) != 0;
	datasetType.qualityOffset = qualityOffset;

	comprSettings.lossy = compressionInfoFlags & DsrcFileFooter::FLAG_LOSSY_QUALITY;
	comprSettings.calculateCrc32 = compressionInfoFlags & DsrcFileFooter::FLAG_CALCULATE_CRC32;
	comprSettings.dnaOrder = dnaOrder;
	comprSettings.qualityOrder = qualityOrder;

	long tagFlags = 0;

	for (int i = 0; i < 13; i++) {
	    tagFlags <<= 8;
	    tagFlags += parameters[i];
	}

	comprSettings.tagPreserveFlags = tagFlags;
}


JNIEXPORT int JNICALL Java_splittablecodec_NativeCodecDecompressor_decompress
  (JNIEnv* env, jobject ths) {

	jbyteArray compressed_direct_buf = static_cast<jbyteArray>(env->GetObjectField(ths, NativeCodecDecompressor_compressedDirectBuf));
	jbyteArray uncompressed_direct_buf = static_cast<jbyteArray>(env->GetObjectField(ths, NativeCodecDecompressor_uncompressedDirectBuf));

	compressed_bytes = static_cast<byte*>(env->GetDirectBufferAddress(compressed_direct_buf));
	uncompressed_bytes = static_cast<byte*>(env->GetDirectBufferAddress(uncompressed_direct_buf));

	int compressed_direct_buf_len = env->GetIntField(ths, NativeCodecDecompressor_compressedDirectBufLen);
	int compressed_direct_buf_off = env->GetIntField(ths, NativeCodecDecompressor_compressedDirectBufOff);

	BlockCompressor superblock(datasetType, comprSettings);

	BitMemoryReader bitMemory(compressed_bytes+compressed_direct_buf_off, compressed_direct_buf_len);

	try {
		superblock.Read(bitMemory, *fastqChunk);
	} catch(std::bad_alloc) {
		std::cout << "Parsing Error." << std::endl;
		exit(1);
	}

	int n = fastqChunk->size;

	std::memcpy(uncompressed_bytes, fastqChunk->data.Pointer(), fastqChunk->size);

	fastqChunk->Reset();

	return n;
}

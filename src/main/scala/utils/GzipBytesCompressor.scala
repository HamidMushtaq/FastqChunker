package utils

import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

class GzipBytesCompressor(bytes: Array[Byte])
{
	def compress() : Array[Byte] = 
	{
		val bos = new ByteArrayOutputStream(bytes.length)
		val gzip = new GZIPOutputStream1(bos)
		gzip.write(bytes)
		gzip.close
		val compressed = bos.toByteArray
		bos.close
		return compressed
	}
}

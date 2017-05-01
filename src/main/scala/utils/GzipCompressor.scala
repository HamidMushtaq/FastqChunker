package utils

import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

class GzipCompressor(data: String)
{
	def compress() : Array[Byte] = 
	{
		val bos = new ByteArrayOutputStream(data.length)
		val gzip = new GZIPOutputStream(bos)
		gzip.write(data.getBytes)
		gzip.close
		val compressed = bos.toByteArray
		bos.close
		return compressed
	}
}

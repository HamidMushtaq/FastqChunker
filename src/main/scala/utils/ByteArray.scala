package utils

class ByteArray(bufSize: Int)
{
	private var bufLen = 0
	private val a: Array[Byte] = new Array[Byte](bufSize)
	
	def getLen() : Int = 
	{
		return bufLen
	}
	
	def copyFrom(src: Array[Byte], si: Int, len: Int) = 
	{
		Array.copy(src, si, a, 0, len)
		bufLen = len
	}
	
	def getArray(): Array[Byte] =
	{
		return a
	}
	
	def getContent(): Array[Byte] = 
	{
		return a.slice(0, bufLen)
	}
}

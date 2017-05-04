package utils

// A wrapper for Array[Byte] with reserved size
class ByteArray(bufSize: Int)
{
	private var bufLen = 0
	private val a: Array[Byte] = new Array[Byte](bufSize)
	
	def getLen() : Int = 
	{
		return bufLen
	}
	
	def setLen(len: Int)
	{
		bufLen = len
	}
	
	def copyFrom(src: Array[Byte], si: Int, len: Int)
	{
		Array.copy(src, si, a, 0, len)
		bufLen = len
	}
	
	def copyFrom(src: ByteArray)
	{
		bufLen = src.getLen
		Array.copy(src.getArray, 0, a, 0, bufLen)
	}
	
	def append(src: ByteArray)
	{
		Array.copy(src.getArray, 0, a, bufLen, src.getLen)
		bufLen += src.getLen
	}
	
	def getArray(): Array[Byte] =
	{
		return a
	}
	
	def getContent(): Array[Byte] = 
	{
		return a.slice(0, bufLen)
	}
	
	def indexOf(c: Char, i: Int) : Int = 
	{
		val ni = a.indexOf(c, i)
		if (ni >= bufLen)
			return -1
		return ni
	} 
}

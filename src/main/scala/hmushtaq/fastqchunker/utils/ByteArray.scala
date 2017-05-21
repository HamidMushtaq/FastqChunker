package hmushtaq.fastqchunker.utils;

import org.apache.commons.lang3.exception.ExceptionUtils

// A wrapper for Array[Byte] with reserved size
class ByteArray(bufSize: Int)
{
	private var bufLen = 0
	private val a: Array[Byte] = new Array[Byte](bufSize)
	
	def makeCopy() : ByteArray =
	{
		val ba = new ByteArray(bufSize)
		ba.copyFrom(a, 0, bufLen)
		return ba
	}
	
	def copyBytes() : Array[Byte] = 
	{
		val arr = new Array[Byte](bufLen)
		
		try
		{
			Array.copy(a, 0, arr, 0, bufLen)
		}
		catch
		{
			case e: Exception => {
				println("!!! Error in ByteArray.copyBytes: This ByteArray's reserved buffer size = " + bufSize + 
					" while number of bytes to copy = " + bufLen + ", Array[Byte]'s size = " + arr.size + "\n" + ExceptionUtils.getStackTrace(e))
				System.exit(1)
			}
		}
		return arr
	}
	
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
		try
		{
			Array.copy(src, si, a, 0, len)
			bufLen = len
		}
		catch
		{
			case e: Exception => {
				println("!!! Error in ByteArray.copyFrom.1: src Array[Byte]'s len = " + len + 
					" while reserved buffer size = " + bufSize + "\n" + ExceptionUtils.getStackTrace(e))
				System.exit(1)
			}
		}
	}
	
	def copyFrom(src: ByteArray)
	{
		try
		{
			Array.copy(src.getArray, 0, a, 0, src.getLen)
			bufLen = src.getLen
		}
		catch
		{
			case e: Exception => {
				println("!!! Error in ByteArray.copyFrom.2: src ByteArray's len = " + src.getLen + 
					" while reserved buffer size = " + bufSize + "\n" + ExceptionUtils.getStackTrace(e))
				System.exit(1)
			}
		}
	}
	
	def append(src: Array[Byte], si: Int, len: Int)
	{
		try
		{
			Array.copy(src, si, a, bufLen, len)
			bufLen += len
		}
		catch
		{
			case e: Exception => {
				println("!!! Error in ByteArray's append.1: bufSize = " + bufSize + 
					", bufLen = " + (bufLen + len) + "\n" + ExceptionUtils.getStackTrace(e))
				System.exit(1)
			}
		}
	}
	
	def append(src: ByteArray)
	{
		try
		{
			Array.copy(src.getArray, 0, a, bufLen, src.getLen)
			bufLen += src.getLen
		}
		catch
		{
			case e: Exception => {
				println("!!! Error in ByteArray's append.2: bufSize = " + bufSize + ", bufLen = " + (bufLen + src.getLen) + 
					"\n" + ExceptionUtils.getStackTrace(e))
				System.exit(1)
			}
		}
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

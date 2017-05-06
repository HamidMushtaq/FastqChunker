import org.apache.commons.lang3.exception.ExceptionUtils
import sys.process._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.io._
import utils._

class PairedFastqChunker(config: Configuration)
{
	val nThreads = config.getNumThreads.toInt
	val numOfChunks = config.getNumChunks.toInt
	val uploadCompressed = config.getUploadCompressed == "true"
	val makeInterleavedChunks = config.getInterleave == "true"
	val minHeaderLength = config.getMinHeaderLength.toInt
	val readLength = config.getReadLength.toInt
	val bufferSize = config.getBlockSizeMB.toInt * 1024 * 1024
	val chunkSize: Int = 60e6.toInt
	
	val readContent = new Array[ByteArray](nThreads)
	val bytesRead = new Array[Int](nThreads)
	val chunkCtr = new Array[Int](nThreads)
	val gzipOutStreams = new Array[GZIPOutputStream1](nThreads)
		
	val inputFileName1 = config.getFastq1Path
	val inputFileName2 = config.getFastq2Path
	val outputFolder = config.getOutputFolder
	val blockSize = 4
	
	def makeChunks()
	{	
		// for fastq1 ////////////////////////////////////////////////////////////
		val fis1 = new FileInputStream(new File(inputFileName1))
		val gis1 = if (inputFileName2.contains(".gz")) new GZIPInput (fis1, bufferSize) else null
		val tmpBufferArray1 = new Array[Byte](bufferSize)
		val bArray1 = new Array[Byte](bufferSize*2)
		// Double buffer
		val bArrayArray1 = new Array[Array[ByteArray]](2)
		var leftOver1: ByteArray = null
		
		// for fastq2 ////////////////////////////////////////////////////////////
		val fis2 = new FileInputStream(new File(inputFileName2))
		val gis2 = if (inputFileName2.contains(".gz")) new GZIPInput (fis2, bufferSize) else null
		val tmpBufferArray2 = new Array[Byte](bufferSize)
		val bArray2 = new Array[Byte](bufferSize*2)
		// Double buffer
		val bArrayArray2 = new Array[Array[ByteArray]](2)
		var leftOver2: ByteArray = null
		//////////////////////////////////////////////////////////////////////
		
		val t0 = System.currentTimeMillis
		
		// Both elements of the double buffer contain a ByteArray for each thread
		for(i <- 0 until 2)
		{
			bArrayArray1(i) = new Array[ByteArray](nThreads)
			bArrayArray2(i) = new Array[ByteArray](nThreads)					
		}
		var bufferArray1 = bArrayArray1(0)
		var bufferArray2 = bArrayArray2(0)
		var bArray1Len = 0
		var bArray2Len = 0
		
		val startTime = System.currentTimeMillis
		var startIndex = 0
		var et: Long = 0
		var endReached = false
		var iter = 0
		var dbi = 0
		///
		val readTime = new SWTimer
		val uploadTime = new SWTimer
		var f: scala.concurrent.Future[Unit] = null
		//
		val maxBytesAtBoundaries = 2*(readLength*2 + minHeaderLength)
		
		while(!endReached)
		{
			val etGlobal = (System.currentTimeMillis - t0) / 1000
			val et = (System.currentTimeMillis - startTime) / 1000
			println(">> elapsed time = " + et + ", global elapsed time = " + etGlobal)
			readTime.start
			for(index <- 0 until nThreads)
			{
				if (endReached)
				{
					bufferArray1(index) = null
					bufferArray2(index) = null
					bytesRead(index) = -1
					println((startIndex + index) + ". End already reached. So ignoring.")
				}
				else
				{
					if (gis1 == null) // If reading uncompressed fastq files 
					{
						bytesRead(index) = fis1.read(tmpBufferArray1)
						fis2.read(tmpBufferArray2)
					}
					else // Reading compressed fastq files
					{
						bytesRead(index) =  gis1.read(tmpBufferArray1)
						gis2.read(tmpBufferArray2)
						println((startIndex + index) + ". gz: Bytes read = " + bytesRead(index))
					}
					
					if (bytesRead(index) == -1) // If end reached
					{
						endReached = true
						bufferArray1(index).copyFrom(leftOver1.getArray, 0, leftOver1.getLen)
						bufferArray2(index).copyFrom(leftOver2.getArray, 0, leftOver2.getLen)
						println((startIndex + index) + ". End reached, bufferArray.size = " + bufferArray1(index).getLen)
					}
					else // If end still not reached
					{
						if (leftOver1 == null) // First iteration
						{
							Array.copy(tmpBufferArray1, 0, bArray1, 0, bytesRead(index))
							bArray1Len = bytesRead(index) 
							leftOver1 = new ByteArray(bufferSize)
							//
							Array.copy(tmpBufferArray2, 0, bArray2, 0, bytesRead(index))
							bArray2Len = bytesRead(index) 
							leftOver2 = new ByteArray(bufferSize)
							
							for(i <- 0 until nThreads)
							{
								bArrayArray1(0)(i) = new ByteArray(bufferSize*2)
								bArrayArray1(1)(i) = new ByteArray(bufferSize*2)
								//
								bArrayArray2(0)(i) = new ByteArray(bufferSize*2)
								bArrayArray2(1)(i) = new ByteArray(bufferSize*2)
								//
								readContent(i) = new ByteArray(2*bufferSize*2)
								chunkCtr(i) = i
								gzipOutStreams(i) = new GZIPOutputStream1(new ByteArrayOutputStream(bufferSize*2))
							}
						}
						else
						{
							Array.copy(leftOver1.getArray, 0, bArray1, 0, leftOver1.getLen)
							Array.copy(tmpBufferArray1, 0, bArray1, leftOver1.getLen, bytesRead(index))
							bArray1Len = leftOver1.getLen + bytesRead(index)
							
							Array.copy(leftOver2.getArray, 0, bArray2, 0, leftOver2.getLen)
							Array.copy(tmpBufferArray2, 0, bArray2, leftOver2.getLen, bytesRead(index))
							bArray2Len = leftOver2.getLen + bytesRead(index)
						}
						
						splitOnReadBoundary(bArray1, bArray1Len, bufferArray1(index), leftOver1)
						splitOnReadBoundary(bArray2, bArray2Len, bufferArray2(index), leftOver2)
						println((startIndex + index) + ". bufferArray1.size = " + bufferArray1(index).getLen + ", leftOver1.size = " + leftOver1.getLen)
						println((startIndex + index) + ". bufferArray2.size = " + bufferArray2(index).getLen + ", leftOver2.size = " + leftOver2.getLen)
						if ((gis1 == null) && (bytesRead(index) < bufferSize))
						{
							println((startIndex + index) + ". Read = " + bytesRead(index) + ", bufferArray.size = " + bufferArray1(index).getLen + 
								", " + bufferArray2(index).getLen)
							endReached = true
						}
					}
				}
			}
			
			println("End reached = " + endReached)
			println(iter + ". Read all " + nThreads + " chunks in the bufferArray, in " + ((System.currentTimeMillis - t0) / 1000) + " secs.")
			readTime.stop
			uploadTime.start
			///////////////////////////////////////////////////////////////////////////////////////
			if (f != null)
			{
				while (!f.isCompleted)
				{
					println("Future still not completed...");
					Thread.sleep(1000);
				}
			}
			f = Future {
				processInterleavedChunks(bufferArray1, bufferArray2, startIndex, endReached)
			}
			//////////////////////////////////////////////////////////////////////////////////////
			uploadTime.stop
			println(iter + " >> uploaded all " + nThreads + " chunks to " + outputFolder + " in " + ((System.currentTimeMillis - t0) / 1000) + " secs.")
			println(iter + " >> read time = " + readTime.getSecsF + ", UPLOAD time = " + uploadTime.getSecsF)
			iter += 1
			startIndex += nThreads
			dbi ^= 1
			bufferArray1 = bArrayArray1(dbi)
			bufferArray2 = bArrayArray2(dbi)
		}
		// Wait for the last iteration to complete
		while (!f.isCompleted)
		{
			println("Future still not completed...");
			Thread.sleep(2000);
		}
		if (gis1 != null)
		{
			gis1.close
			gis2.close
		}
		else
		{
			fis1.close
			fis2.close
		}
	}

	private def processInterleavedChunks(bufferArray1: Array[ByteArray], bufferArray2: Array[ByteArray], chunkStart: Int, endReached: Boolean) =
	{
		val threadArray = new Array[Thread](nThreads)
		
		for(threadIndex <- 0 until nThreads)
		{
			threadArray(threadIndex) = new Thread 
			{
				override def run 
				{
					val ba1: ByteArray = bufferArray1(threadIndex)
					if ((bufferArray1(threadIndex) != null) && (bufferArray1(threadIndex).getLen > 1))
					{
						val cn = chunkStart + threadIndex
						
						//println("Interleaving...")
						interleave(bufferArray1(threadIndex), bufferArray2(threadIndex), readContent(threadIndex))
						gzipOutStreams(threadIndex).write(readContent(threadIndex).getArray, 0, readContent(threadIndex).getLen)
						gzipOutStreams(threadIndex).flush
						if ((gzipOutStreams(threadIndex).getSize > chunkSize) || endReached)
						{
							HDFSManager.writeWholeBinFile(outputFolder + "/" + chunkCtr(threadIndex) + ".fq.gz", 
								gzipOutStreams(threadIndex).getByteArray)
							chunkCtr(threadIndex) += nThreads
							gzipOutStreams(threadIndex).reset
						}							
					}
				}
			}
			threadArray(threadIndex).start
		}
		
		for(threadIndex <- 0 until nThreads)
			threadArray(threadIndex).join
	}
	
	private def interleave(ba1: ByteArray, ba2: ByteArray, rba: ByteArray)
	{
		var startIndex = 0
		var rIndex = 0
		var index = 0
		var done = false
		val stride = readLength*2 + minHeaderLength - 10 // -10 to take care of anamolous data
		
		while(index != -1)
		{
			index = ba1.indexOf('\n', index + stride) 
			if (index != -1)
			{
				val numOfElem = index - startIndex + 1
				System.arraycopy(ba1.getArray, startIndex, rba.getArray, rIndex, numOfElem)
				rIndex += numOfElem
				System.arraycopy(ba2.getArray, startIndex, rba.getArray, rIndex, numOfElem)
				rIndex += numOfElem
				rba.setLen(rIndex)
				
				startIndex = index+1
			}
		}
	}
	
	private def splitOnReadBoundary(ba: Array[Byte], baSize: Int, retArray: ByteArray, leftOver: ByteArray)
	{
		var ei = baSize-1
		var lastByte = ba(ei)
		var secLastByte = ba(ei-1)
		var numOfNewLines = 0
		
		try
		{
			// Find "\n+" first
			while(!(lastByte == '\n' && secLastByte == '+'))
			{
				ei -= 1
				lastByte = ba(ei)
				secLastByte = ba(ei-1)
			}
			
			numOfNewLines = 0
			ei -= 1
			while(numOfNewLines < 3)
			{
				if (ei < 0)
				{
					retArray.copyFrom(ba, 0, 0)
					leftOver.copyFrom(ba, 0, baSize)
				}
				if (ba(ei) == '\n')
					numOfNewLines += 1
				ei -=1 // At the end, this would be the index of character just before '\n'
			}
		}
		catch 
		{
			case e: Exception => 
			{
				println("ba.size = " + baSize)
				println("ei = " + ei)
				println("numOfNewLines = " + numOfNewLines)
				println("\n>> Exception: " + ExceptionUtils.getStackTrace(e) + "!!!\n") 
				System.exit(1)
			}
		}
		
		retArray.copyFrom(ba, 0, ei+2)
		leftOver.copyFrom(ba, ei+2, baSize - (ei+2))
	}
}

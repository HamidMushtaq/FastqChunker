package hmushtaq.fastqchunker.chunkers

import org.apache.commons.lang3.exception.ExceptionUtils
import sys.process._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.zip.GZIPInputStream
import java.io._
import hmushtaq.fastqchunker.utils._
	
class PairedFastqChunker(config: Configuration) extends SingleFastqChunker(config)
{
	protected val interleave = config.getInterleave.toBoolean
	protected val inputFileName2 = config.getFastq2Path
	protected val gzipOutStreams2 = if (interleave) null else new Array[GZIPOutputStream1](nThreads)
	protected val baFuture2 = new Array[ByteArray](nThreads)
	protected val (minHeaderLength, readLength) = getReadAndHeaderLength()
	
	override def makeChunks()
	{	
		val bytesRead = new Array[Int](nThreads)
		// for fastq1 ////////////////////////////////////////////////////////////
		val fis1 = new FileInputStream(new File(inputFileName))
		val gis1 = if (inputFileName.contains(".gz")) new GZIPInput (fis1, bufferSize) else null
		val tmpBufferArray1 = new Array[Byte](bufferSize)
		val bArray1 = new ByteArray(bufferSize*2)
		val bArrayArray1 = new Array[ByteArray](nThreads)
		var leftOver1: ByteArray = null
		// for fastq2 ////////////////////////////////////////////////////////////
		val fis2 = new FileInputStream(new File(inputFileName2))
		val gis2 = if (inputFileName2.contains(".gz")) new GZIPInput (fis2, bufferSize) else null
		val tmpBufferArray2 = new Array[Byte](bufferSize)
		val bArray2 = new ByteArray(bufferSize*2)
		val bArrayArray2 = new Array[ByteArray](nThreads)
		var leftOver2: ByteArray = null
		//////////////////////////////////////////////////////////////////////
		
		val t0 = System.currentTimeMillis
		
		for(ti <- 0 until nThreads)
		{
			baFuture(ti) = new ByteArray(bufferSize*2)
			baFuture2(ti) = new ByteArray(bufferSize*2)
			bArrayArray1(ti) = new ByteArray(bufferSize*2)
			bArrayArray2(ti) = new ByteArray(bufferSize*2)
			if (!interleave)
				gzipOutStreams2(ti) = new GZIPOutputStream1(new ByteArrayOutputStream, compLevel)
		}
		
		var startIndex = 0
		var endReached = false
		var iter = 0
		var totalBytesRead: Long = 0
	
		val f = new Array[Future[Unit]](nThreads)
	
		while(!endReached)
		{
			for(index <- 0 until nThreads)
			{
				if (endReached) // This condition will be reached for eg. when end was reached (if bytesRead(index) == -1) on index = 7, and now index > 7.
				{
					bArrayArray1(index) = null
					bArrayArray2(index) = null
					bytesRead(index) = -1
					println((startIndex + index) + ". End already reached. So ignoring.")
				}
				else
				{
					// Read from the fastq files /////////////////////////////
					if (gis1 == null) // If reading uncompressed fastq files 
					{
						bytesRead(index) = fis1.read(tmpBufferArray1)
						fis2.read(tmpBufferArray2)
					}
					else // Reading compressed fastq files
					{
						bytesRead(index) =  gis1.read(tmpBufferArray1)
						gis2.read(tmpBufferArray2)
					}
					//////////////////////////////////////////////////////////
					
					if (bytesRead(index) == -1) // -1 means that no more bytes were read. That is, end was reached.
					{
						endReached = true
						bArrayArray1(index).copyFrom(leftOver1)
						bArrayArray2(index).copyFrom(leftOver2)
						println((startIndex + index) + ". End reached, bArrayArray.size = " + bArrayArray1(index).getLen)
					}
					else // If end still not reached
					{
						if (leftOver1 == null) // First ever iteration (iter = 0, index = 0)
						{
							bArray1.copyFrom(tmpBufferArray1, 0, bytesRead(index))
							leftOver1 = new ByteArray(bufferSize)
							//
							bArray2.copyFrom(tmpBufferArray2, 0, bytesRead(index))
							leftOver2 = new ByteArray(bufferSize)
						}
						else // Not the first ever iteration -> !(iter == 0 && index == 0)
						{
							bArray1.copyFrom(leftOver1)
							bArray1.append(tmpBufferArray1, 0, bytesRead(index))
							//
							bArray2.copyFrom(leftOver2)
							bArray2.append(tmpBufferArray2, 0, bytesRead(index))
						}
						
						splitAtReadBoundary(bArray1, bArrayArray1(index), leftOver1)
						splitAtReadBoundary(bArray2, bArrayArray2(index), leftOver2)
						totalBytesRead += 2*bArrayArray1(index).getLen
					}
				}
				//////////////////////////////////////////////////////////////
				if (f(index) != null)
					Await.result(f(index), Duration.Inf)
				gzipOutStreams(index).synchronized
				{
					if (bArrayArray1(index) == null)
					{
						baFuture(index).setLen(0)
						baFuture2(index).setLen(0)
					}
					else
					{
						baFuture(index).copyFrom(bArrayArray1(index))
						baFuture2(index).copyFrom(bArrayArray2(index))
					}
				}
				f(index) = Future {
					gzipOutStreams(index).synchronized
					{
						if (interleave)
							writeInterleavedChunk(index)
						else
							writePairedChunks(index)
					}
				}
				//////////////////////////////////////////////////////////////
			}
			
			val et = ((System.currentTimeMillis - t0) / 1000)
			println(s"$iter. Read ${totalBytesRead.toFloat / (1024 * 1024 * 1024)} GBs in ${et / 60} mins ${et % 60} secs.")
			iter += 1
			startIndex += nThreads
		}
		println("Writing the last remaining bytes...")
		for(ti <- 0 until nThreads)
		{
			var et = (System.currentTimeMillis - t0) / 1000
			Await.result(f(ti), Duration.Inf)
			et = (System.currentTimeMillis - t0) / 1000
			gzipOutStreams(ti).close
			if (!interleave)
				gzipOutStreams2(ti).close
			if (gzipOutStreams(ti).getSize > MIN_ZIP_FILE_SIZE)
			{
				if (interleave)
					HDFSManager.writeWholeBinFile(outputFolder + "/" + chunkCtr(ti) + ".fq.gz", gzipOutStreams(ti).getByteArray)
				else
				{
					HDFSManager.writeWholeBinFile(outputFolder + "/" + chunkCtr(ti) + "-1.fq.gz", gzipOutStreams(ti).getByteArray)
					HDFSManager.writeWholeBinFile(outputFolder + "/" + chunkCtr(ti) + "-2.fq.gz", gzipOutStreams2(ti).getByteArray)
				}
				val s = "ti: " + ti + ", " + gzipOutStreams(ti).getSize + " bytes\n"
				writeWholeFile(outputFolder + "/ulStatus/" + chunkCtr(ti), s)
			}
		}
		// Wait for the last iteration to complete
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
		writeWholeFile(outputFolder + "/ulStatus/end.txt", "")
	}

	private def writeInterleavedChunk(ti: Int)
	{
		if (baFuture(ti).getLen != 0)
		{
			gzipOutStreams(ti).write(interleave(baFuture(ti), baFuture2(ti)))
			val numOfBytes = gzipOutStreams(ti).getSize 
			if (numOfBytes > chunkSize)
			{
				gzipOutStreams(ti).close
				writeWholeBinFile(outputFolder + "/" + chunkCtr(ti) + ".fq.gz", gzipOutStreams(ti).getByteArray)
				val s = "ti: " + ti + ", " + (numOfBytes / 1e6.toInt).toString + " MB\n"
				writeWholeFile(outputFolder + "/ulStatus/" + chunkCtr(ti), s)
				
				chunkCtr(ti) += nThreads
				gzipOutStreams(ti) = new GZIPOutputStream1(new ByteArrayOutputStream, compLevel)
			}							
		}
	}
	
	private def writePairedChunks(ti: Int)
	{
		if (baFuture(ti).getLen != 0)
		{
			gzipOutStreams(ti).write(baFuture(ti).getArray, 0, baFuture(ti).getLen)
			gzipOutStreams2(ti).write(baFuture2(ti).getArray, 0, baFuture2(ti).getLen)
			val numOfBytes = gzipOutStreams(ti).getSize 
			if (numOfBytes > chunkSize)
			{
				gzipOutStreams(ti).close
				gzipOutStreams2(ti).close
				writeWholeBinFile(outputFolder + "/" + chunkCtr(ti) + "-1.fq.gz", gzipOutStreams(ti).getByteArray)
				writeWholeBinFile(outputFolder + "/" + chunkCtr(ti) + "-2.fq.gz", gzipOutStreams2(ti).getByteArray)
				val s = "ti: " + ti + ", " + (numOfBytes / 1e6.toInt).toString + " MB\n"
				writeWholeFile(outputFolder + "/ulStatus/" + chunkCtr(ti), s)
				
				chunkCtr(ti) += nThreads
				gzipOutStreams(ti) = new GZIPOutputStream1(new ByteArrayOutputStream, compLevel)
				gzipOutStreams2(ti) = new GZIPOutputStream1(new ByteArrayOutputStream, compLevel)
			}							
		}
	}
	
	private def interleave(ba1: ByteArray, ba2: ByteArray) : Array[Byte] =
	{
		var startIndex = 0
		var rIndex = 0
		var index = 0
		var done = false
		val stride = readLength*2 + minHeaderLength - 10 // -10 to take care of anamolous data
		val interleavedContent = new Array[Byte](ba1.getLen*2)
		
		while(index != -1)
		{
			index = ba1.indexOf('\n', index + stride) 
			if (index != -1)
			{
				val numOfElem = index - startIndex + 1
				System.arraycopy(ba1.getArray, startIndex, interleavedContent, rIndex, numOfElem)
				rIndex += numOfElem
				System.arraycopy(ba2.getArray, startIndex, interleavedContent, rIndex, numOfElem)
				rIndex += numOfElem
				
				startIndex = index+1
			}
		}
		
		return interleavedContent
	}
	
	private def getReadAndHeaderLength() : (Int, Int) = 
	{
		val br = {
			if (inputFileName.contains(".gz"))
				new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(inputFileName))))
			else
				new BufferedReader(new FileReader(inputFileName))
		}
				
		val headerLen = br.readLine.size
		val readLen = br.readLine.size
		br.close
		
		(headerLen, readLen)
	}
}

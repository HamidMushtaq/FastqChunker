import org.apache.commons.lang3.exception.ExceptionUtils
import sys.process._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.zip.GZIPInputStream
import java.io._
import utils._

class PairedFastqChunker(config: Configuration)
{
	val nThreads = config.getNumThreads.toInt
	val bufferSize = config.getBlockSizeMB.toInt * 1024 * 1024
	val chunkSize: Int = config.getChunkSizeMB.toInt * 1024 * 1024
	val compLevel = config.getCompLevel.toInt
	
	val readContent = new Array[ByteArray](nThreads)
	val bytesRead = new Array[Int](nThreads)
	val chunkCtr = new Array[Int](nThreads)
	val gzipOutStreams = new Array[GZIPOutputStream1](nThreads)
		
	val inputFileName1 = config.getFastq1Path
	val inputFileName2 = config.getFastq2Path
	val outputFolder = config.getOutputFolder
	
	val (minHeaderLength, readLength) = getReadAndHeaderLength()
	
	def makeChunks()
	{	
		// for fastq1 ////////////////////////////////////////////////////////////
		val fis1 = new FileInputStream(new File(inputFileName1))
		val gis1 = if (inputFileName2.contains(".gz")) new GZIPInput (fis1, bufferSize) else null
		val tmpBufferArray1 = new Array[Byte](bufferSize)
		val bArray1 = new ByteArray(bufferSize*2)
		// Double buffer
		val bArrayArrayBuf1 = new Array[Array[ByteArray]](2)
		var leftOver1: ByteArray = null
		
		// for fastq2 ////////////////////////////////////////////////////////////
		val fis2 = new FileInputStream(new File(inputFileName2))
		val gis2 = if (inputFileName2.contains(".gz")) new GZIPInput (fis2, bufferSize) else null
		val tmpBufferArray2 = new Array[Byte](bufferSize)
		val bArray2 = new ByteArray(bufferSize*2)
		// Double buffer
		val bArrayArrayBuf2 = new Array[Array[ByteArray]](2)
		var leftOver2: ByteArray = null
		//////////////////////////////////////////////////////////////////////
		
		val t0 = System.currentTimeMillis
		
		// Both elements of the double buffer contain a ByteArray for each thread
		for(i <- 0 until 2)
		{
			bArrayArrayBuf1(i) = new Array[ByteArray](nThreads)
			bArrayArrayBuf2(i) = new Array[ByteArray](nThreads)					
		}
		var bArrayArray1 = bArrayArrayBuf1(0)
		var bArrayArray2 = bArrayArrayBuf2(0)
		
		val startTime = System.currentTimeMillis
		var startIndex = 0
		var et: Long = 0
		var endReached = false
		var iter = 0
		var dbi = 0
		///
		val readTime = new SWTimer
		val uploadTime = new SWTimer
		var f: Seq[Future[(Int, Int)]] = null
	
		while(!endReached)
		{
			val etGlobal = (System.currentTimeMillis - t0) / 1000
			val et = (System.currentTimeMillis - startTime) / 1000
			println(">> elapsed time = " + et + ", global elapsed time = " + etGlobal)
			readTime.start
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
						println((startIndex + index) + ". gz: Bytes read = " + bytesRead(index))
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
							
							for(i <- 0 until nThreads)
							{
								for(e <- 0 until 2)
								{
									bArrayArrayBuf1(e)(i) = new ByteArray(bufferSize*2)
									bArrayArrayBuf2(e)(i) = new ByteArray(bufferSize*2)
								}
								//
								readContent(i) = new ByteArray(2*bufferSize*2)
								chunkCtr(i) = i
								gzipOutStreams(i) = new GZIPOutputStream1(new ByteArrayOutputStream(bufferSize*2), compLevel)
							}
						}
						else // Not the first ever iteration -> !(iter == 0 && index == 0)
						{
							bArray1.copyFrom(leftOver1)
							bArray1.append(tmpBufferArray1, 0, bytesRead(index))
							//
							bArray2.copyFrom(leftOver2)
							bArray2.append(tmpBufferArray2, 0, bytesRead(index))
						}
						
						splitOnReadBoundary(bArray1, bArrayArray1(index), leftOver1)
						splitOnReadBoundary(bArray2, bArrayArray2(index), leftOver2)
						println((startIndex + index) + " -> bArrayArray1.size = " + bArrayArray1(index).getLen + ", leftOver1.size = " + leftOver1.getLen)
						println((startIndex + index) + " -> bArrayArray2.size = " + bArrayArray2(index).getLen + ", leftOver2.size = " + leftOver2.getLen)
						if ((gis1 == null) && (bytesRead(index) < bufferSize))
						{
							println((startIndex + index) + " -> Read = " + bytesRead(index) + ", bArrayArray.size = " + bArrayArray1(index).getLen + 
								", " + bArrayArray2(index).getLen)
							endReached = true
						}
					}
				}
			}
			
			println("End reached = " + endReached)
			println(iter + ". Read all " + nThreads + " chunks in the bArrayArray, in " + ((System.currentTimeMillis - t0) / 1000) + " secs.")
			readTime.stop
			uploadTime.start
			///////////////////////////////////////////////////////////////////////////////////////
			if (f != null)
			{
				val r: Seq[(Int, Int)] = Await.result(Future.sequence(f), Duration.Inf)
				println("r: " + r)
			}
			f = for (ti <- 0 until nThreads) yield Future {
				val ret = interleaveAndWrite(bArrayArray1(ti), bArrayArray2(ti), ti, endReached)
				ret
			}
			//////////////////////////////////////////////////////////////////////////////////////
			uploadTime.stop
			println(iter + ". Uploaded all " + nThreads + " chunks to " + outputFolder + " in " + ((System.currentTimeMillis - t0) / 1000) + " secs.")
			println(iter + ". Read time = " + readTime.getSecsF + ", UPLOAD time = " + uploadTime.getSecsF)
			iter += 1
			startIndex += nThreads
			dbi ^= 1
			bArrayArray1 = bArrayArrayBuf1(dbi)
			bArrayArray2 = bArrayArrayBuf2(dbi)
		}
		// Wait for the last iteration to complete
		val r: Seq[(Int, Int)] = Await.result(Future.sequence(f), Duration.Inf)
		println("r: " + r)
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

	private def interleaveAndWrite(bArray1: ByteArray, bArray2: ByteArray, ti: Int, endReached: Boolean) : (Int, Int) =
	{
		var r: (Int, Int) = (0,0)
		if ((bArray1 != null) && (bArray1.getLen > 1))
		{
			interleave(bArray1, bArray2, readContent(ti))
			gzipOutStreams(ti).write(readContent(ti).getArray, 0, readContent(ti).getLen)
			gzipOutStreams(ti).flush
			val numOfBytes = gzipOutStreams(ti).getSize 
			r = (chunkCtr(ti), numOfBytes / 1e6.toInt) 
			if ((numOfBytes > chunkSize) || endReached)
			{
				val os = gzipOutStreams(ti).getOutputStream
				gzipOutStreams(ti).close
				HDFSManager.writeWholeBinFile(outputFolder + "/" + chunkCtr(ti) + ".fq.gz", gzipOutStreams(ti).getByteArray)
				val s = "ti: " + ti + ", " + (numOfBytes / 1e6.toInt).toString + " MB\n"
				HDFSManager.writeWholeFile(outputFolder + "/ulStatus/" + chunkCtr(ti), s)
				if (!endReached)
				{
					os.reset
					gzipOutStreams(ti) = new GZIPOutputStream1(os, compLevel)
					chunkCtr(ti) += nThreads
				}
				else if (ti == (nThreads-1)) // endReached && ti == nThreads-1
					HDFSManager.writeWholeFile(outputFolder + "/ulStatus/end.txt", chunkCtr(ti).toString + "\n")
			}							
		}
		else if (endReached)
		{
			val numOfBytes = gzipOutStreams(ti).getSize 
			gzipOutStreams(ti).close
			HDFSManager.writeWholeBinFile(outputFolder + "/" + chunkCtr(ti) + ".fq.gz", gzipOutStreams(ti).getByteArray)
			r = (chunkCtr(ti), numOfBytes / 1e6.toInt)
			if (ti == (nThreads-1))
				HDFSManager.writeWholeFile(outputFolder + "/ulStatus/end.txt", chunkCtr(ti).toString + "\n")
		}
		r
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
	
	private def splitOnReadBoundary(byteArray: ByteArray, retArray: ByteArray, leftOver: ByteArray)
	{
		val ba = byteArray.getArray
		val baSize = byteArray.getLen
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
	
	private def getReadAndHeaderLength() : (Int, Int) = 
	{
		val gzip = new GZIPInputStream(new FileInputStream(inputFileName1));
		val br = new BufferedReader(new InputStreamReader(gzip));
		
		val headerLen = br.readLine.size
		val readLen = br.readLine.size
		br.close
		
		(headerLen, readLen)
	}
}

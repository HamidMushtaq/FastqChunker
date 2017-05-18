import org.apache.commons.lang3.exception.ExceptionUtils
import sys.process._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.zip.GZIPInputStream
import java.io._
import utils._

class SingleFastqChunker(config: Configuration)
{
	val nThreads = config.getNumThreads.toInt
	val bufferSize = config.getBlockSizeMB.toInt * 1024 * 1024
	val chunkSize: Int = config.getChunkSizeMB.toInt * 1024 * 1024
	val compLevel = config.getCompLevel.toInt
	val interleave = config.getInterleave.toBoolean
	val inputFileName = config.getFastq1Path
	val outputFolder = config.getOutputFolder

	val gzipOutStreams = new Array[GZIPOutputStream1](nThreads)
	val chunkCtr = new Array[Int](nThreads)
	for(ti <- 0 until nThreads)
	{
		gzipOutStreams(ti) = new GZIPOutputStream1(new ByteArrayOutputStream(bufferSize*2), compLevel)
		chunkCtr(ti) = ti
	}
	
	def makeChunks()
	{	
		val fis = new FileInputStream(new File(inputFileName))
		val gis = if (inputFileName.contains(".gz")) new GZIPInput (fis, bufferSize) else null
		val tmpBufferArray = new Array[Byte](bufferSize)
		val bytesRead = new Array[Int](nThreads)
		val bArray = new ByteArray(bufferSize*2)
		// Double buffer
		val bArrayArrayBuf = new Array[Array[ByteArray]](2)
		var leftOver: ByteArray = null
		
		val t0 = System.currentTimeMillis
		
		// Both elements of the double buffer contain a ByteArray for each thread
		for(i <- 0 until 2)
			bArrayArrayBuf(i) = new Array[ByteArray](nThreads)
			
		var bArrayArray = bArrayArrayBuf(0)
		
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
					bArrayArray(index) = null
					bytesRead(index) = -1
					println((startIndex + index) + ". End already reached. So ignoring.")
				}
				else
				{
					// Read from the fastq file /////////////////////////////
					if (gis == null) // If reading uncompressed fastq file
						bytesRead(index) = fis.read(tmpBufferArray)
					else // Reading compressed fastq file
						bytesRead(index) =  gis.read(tmpBufferArray)
						
					if (bytesRead(index) == -1) // -1 means that no more bytes were read. That is, end was reached.
					{
						endReached = true
						bArrayArray(index).copyFrom(leftOver)
						println((startIndex + index) + ". End reached, bArrayArray.size = " + bArrayArray(index).getLen)
					}
					else // If end still not reached
					{
						if (leftOver == null) // First ever iteration (iter = 0, index = 0)
						{
							bArray.copyFrom(tmpBufferArray, 0, bytesRead(index))
							leftOver = new ByteArray(bufferSize)
							
							for(i <- 0 until nThreads)
							{
								for(e <- 0 until 2)
									bArrayArrayBuf(e)(i) = new ByteArray(bufferSize*2)
							}
						}
						else // Not the first ever iteration -> !(iter == 0 && index == 0)
						{
							bArray.copyFrom(leftOver)
							bArray.append(tmpBufferArray, 0, bytesRead(index))
						}
						
						bArrayArray(index).synchronized
						{
							ReadBoundarySplitter.split(bArray, bArrayArray(index), leftOver)
						}
						println((startIndex + index) + " -> bArrayArray.size = " + bArrayArray(index).getLen + ", leftOver.size = " + leftOver.getLen)
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
				val ret = {
					gzipOutStreams(ti).synchronized
					{
						writeChunk(bArrayArray(ti), ti, endReached)
					}
				}
				ret
			}
			//////////////////////////////////////////////////////////////////////////////////////
			uploadTime.stop
			println(iter + ". Uploaded all " + nThreads + " chunks to " + outputFolder + " in " + ((System.currentTimeMillis - t0) / 1000) + " secs.")
			println(iter + ". Read time = " + readTime.getSecsF + ", UPLOAD time = " + uploadTime.getSecsF)
			iter += 1
			startIndex += nThreads
			dbi ^= 1
			bArrayArray = bArrayArrayBuf(dbi)
		}
		// Wait for the last iteration to complete
		val r: Seq[(Int, Int)] = Await.result(Future.sequence(f), Duration.Inf)
		println("r: " + r)
		if (gis != null)
			gis.close
		else
			fis.close
		HDFSManager.writeWholeFile(outputFolder + "/ulStatus/end.txt", "")
	}

	private def writeChunk(bArray: ByteArray, ti: Int, endReached: Boolean) : (Int, Int) =
	{
		var r: (Int, Int) = (0,0)
		
		if ((bArray != null) && (bArray.getLen > 1))
		{
			bArray.synchronized
			{
				gzipOutStreams(ti).write(bArray.getArray, 0, bArray.getLen)
			}
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
			}							
		}
		else if (endReached)
		{
			val numOfBytes = gzipOutStreams(ti).getSize 
			gzipOutStreams(ti).close
			HDFSManager.writeWholeBinFile(outputFolder + "/" + chunkCtr(ti) + ".fq.gz", gzipOutStreams(ti).getByteArray)
			val s = "ti: " + ti + ", " + (numOfBytes / 1e6.toInt).toString + " MB\n"
			HDFSManager.writeWholeFile(outputFolder + "/ulStatus/" + chunkCtr(ti), s)
			r = (chunkCtr(ti), numOfBytes / 1e6.toInt)
		}
		r
	}
}

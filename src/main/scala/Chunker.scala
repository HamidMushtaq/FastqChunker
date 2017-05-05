/****************************************/
//	Program:	DNASeqAnalyzer.scala	
//	Author:		Hamid Mushtaq  		
//	Company:	TU Delft	 	
/****************************************/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.commons.lang3.exception.ExceptionUtils
import sys.process._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.util.zip.GZIPInputStream;

import java.io._
import utils._

object Chunker
{
	// Attributes
	var sizeMB = 0
	var nThreads = 0
	var numOfChunks = 0
	var uploadCompressed = false
	var makeInterleavedChunks = false
	var minHeaderLength = 0
	var readLength = 0
	var bufferSize = 0 
	var useFiles = false
	val chunkSize = 192e6
	///////////////////////////////////////////////////
	val useReadAndHeaderLenForInterleaving = true
	var chunkCtr: Array[Int] = null
	var bytesCtr: Array[Int] = null
	var gzipOutStreams: Array[GZIPOutputStream1] = null
	var data: Array[ByteArray] = null
	var readContent: Array[ByteArray] = null
	var blockSize = 8
	val tmpDir = "/home/hamidmushtaq/halvade/spark/scala/chunkers/mirror/zipTmp/"
	val t0 = System.currentTimeMillis
	
	def processChunks(bufferArray: Array[ByteArray], chunkStart: Int, suffix: String, outputFolder: String, 
		streamMap: scala.collection.mutable.HashMap[String, PrintWriter]) =
	{
		val threadArray = new Array[Thread](nThreads)
		val suffixStr = if (suffix == "") "" else ("-" + suffix)
		
		for(threadIndex <- 0 until nThreads)
		{
			threadArray(threadIndex) = new Thread 
			{
				override def run 
				{
					if ((bufferArray(threadIndex) != null) && (bufferArray(threadIndex).getLen > 1))
					{
						val cn = chunkStart + threadIndex
						if (uploadCompressed)
						{
							val compressedBytes = new GzipBytesCompressor(bufferArray(threadIndex).getContent).compress
							HDFSManager.writeWholeBinFile(outputFolder + "/" + cn + suffixStr + ".fq.gz", compressedBytes)
						}
						else
						{
							try
							{
								val chunkNum = threadIndex // cn % numOfChunks
								val round = chunkStart / nThreads
								val numOfRounds = numOfChunks / nThreads
								val chunkID = threadIndex + "_" + (round % numOfRounds)
								if (!streamMap.contains(chunkID))
								{
									streamMap(chunkID) = HDFSManager.openStream(outputFolder + "/" + chunkID + suffixStr + ".fq")
								}
								else
								{
									val content = new String(bufferArray(threadIndex).getContent)
									streamMap(chunkID).write(new String(bufferArray(threadIndex).getContent))
								}
							}
							catch 
							{
								case e: Exception => println("\n>> Exception: " + ExceptionUtils.getStackTrace(e) + "!!!\n") 
							}
						}
					}
				}
			}
			threadArray(threadIndex).start
		}
		
		for(threadIndex <- 0 until nThreads)
			threadArray(threadIndex).join
	}
	
	def interleave(ba1: ByteArray, ba2: ByteArray, rba: ByteArray)
	{
		var startIndex = 0
		var rIndex = 0
		var index = 0
		var done = false
		val stride = readLength*2 + minHeaderLength - 10 // -10 to take care of anamolous data

		if (useReadAndHeaderLenForInterleaving)
		{
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
		else
		{
			while(index != -1)
			{
				index = ba1.indexOf('\n', index + stride) 
				if (index != -1)
				{
					index = ba1.indexOf('\n', index+1)
					index = ba1.indexOf('\n', index+1)
					index = ba1.indexOf('\n', index+1)
					
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
	}
	
	def processInterleavedChunks(bufferArray1: Array[ByteArray], bufferArray2: Array[ByteArray], chunkStart: Int, outputFolder: String, 
		streamMap: scala.collection.mutable.HashMap[String, PrintWriter], endReached: Boolean) =
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
						if (uploadCompressed)
						{
							//println("Interleaving...")
							interleave(bufferArray1(threadIndex), bufferArray2(threadIndex), readContent(threadIndex))
							if (useFiles)
							{
								gzipOutStreams(threadIndex).write(readContent(threadIndex).getArray, 0, readContent(threadIndex).getLen)
								bytesCtr(threadIndex) += readContent(threadIndex).getLen
								if ((bytesCtr(threadIndex) > chunkSize) || endReached)
								{
									gzipOutStreams(threadIndex).close
									bytesCtr(threadIndex) = 0
									HDFSManager.upload(chunkCtr(threadIndex) + ".fq.gz", tmpDir, outputFolder)
									chunkCtr(threadIndex) += nThreads
									if (!endReached)
										gzipOutStreams(threadIndex) = new GZIPOutputStream1(
											new FileOutputStream(tmpDir + chunkCtr(threadIndex) + ".fq.gz"))
								}
							}
							else
							{
								data(threadIndex).append(readContent(threadIndex))
								if ((data(threadIndex).getLen > chunkSize) || endReached)
								{
									val compressedBytes = new GzipBytesCompressor(data(threadIndex).getArray).compress(data(threadIndex).getLen)
									HDFSManager.writeWholeBinFile(outputFolder + "/" + chunkCtr(threadIndex) + ".fq.gz", compressedBytes)
									data(threadIndex).setLen(0)
									chunkCtr(threadIndex) += nThreads
								}
							}
						}
						else
						{
							try
							{
								val chunkNum = threadIndex // cn % numOfChunks
								val round = chunkStart / nThreads
								val numOfRounds = numOfChunks / nThreads
								val chunkID = threadIndex + "_" + (round % numOfRounds)
								if (!streamMap.contains(chunkID))
								{
									//println(">> Opening stream " + chunkID)
									streamMap(chunkID) = HDFSManager.openStream(outputFolder + "/" + chunkID + ".fq")
								}
								else
								{
									interleave(bufferArray1(threadIndex), bufferArray2(threadIndex), readContent(threadIndex))
									val content = new String(readContent(threadIndex).getContent)
									//println(">> Stream " + chunkID + " is already open. Writing content of size " + content.size)
									streamMap(chunkID).write(content)
								}
							}
							catch 
							{
								case e: Exception => println("\n>> Exception: " + ExceptionUtils.getStackTrace(e) + "!!!\n") 
							}
						}
					}
				}
			}
			threadArray(threadIndex).start
		}
		
		for(threadIndex <- 0 until nThreads)
			threadArray(threadIndex).join
	}
	
	def splitOnReadBoundary(ba: Array[Byte], baSize: Int, retArray: ByteArray, leftOver: ByteArray)
	{
		//println("HAMID: baSize = " + baSize + ", ba.size = " + ba.size)
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
	
	def splitOnReadBoundary(ba: Array[Byte], retArray: ByteArray, leftOver: ByteArray)
	{
		splitOnReadBoundary(ba, ba.length, retArray, leftOver)
	}
	
	// Testing of splitOnReadBoundary ////////////////////////////////////////
	def testSplitOnReadBoundary()
	{
		val s = new String(
		"@ST-E00118:53:H02GVALXX:1:1101:4034:1555 1:N:0:0\n" +
		"CTCCCTGCCTAAAATCATGTCCACNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN\n+\n" +
		"AAAFFJA7FFJJJJJJJJJJJF<A###############################################################################################################################\n" +
		"@ST-E00118:53:H02GVALXX:1:1101:2350:1555 2:N:0:0\n" +
		"AAAAACAACCCCATCAAAAAGTGGNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN\n+\n" +
		"AA-AFJJJJJ<JJJFJJF<AJJJJ###############################################################################################################################\n" +
		"@ST-E00118:53:H02GVALXX:1:1101:4034:1555 1:N:0:0\n" +
		"CTCCCTGCCTAAAATCATGTCCACNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN\n+\n" +
		"AAAFFJA7FFJJJJJJJJJJJF<A###############################################################################################################################\n" +
		"@ST-E00118:53:H02GVALXX:1:1101:4420:1555 2:N:0:0\n" +
		"ATGCAGGGGTTAGAATTGCTTGTGNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN\n")
		val bytes = s.toString.getBytes
		val leftOver1 = new ByteArray(bufferSize)
		///////////////////////////////////////////////////
		val t0 = System.currentTimeMillis
		val r = new ByteArray(bufferSize*2)
		splitOnReadBoundary(bytes, r, leftOver1)
		val et = System.currentTimeMillis - t0
		//////////////////////////////////////////////////
		println(s + '\n')
		val upperStr = new String(r.getContent)
		val lowerStr = new String(leftOver1.getContent)
		println("upperStr = {\n" + upperStr + "}\n")
		println("=================================================")
		println("lowerStr = {\n" + lowerStr + "}\n")
		
		println("Size = " + s.size + " bytes.\nExecution time: " + et + " ms")
	}
	//////////////////////////////////////////////////////////////////////////
	
	def setAttributes(config: Configuration)
	{
		nThreads = config.getNumThreads.toInt
		numOfChunks = config.getNumChunks.toInt
		uploadCompressed = config.getUploadCompressed == "true"
		makeInterleavedChunks = config.getInterleave == "true"
		minHeaderLength = config.getMinHeaderLength.toInt
		readLength = config.getReadLength.toInt
		bufferSize = config.getBlockSizeMB.toInt * 1024 * 1024
		
		println(s"nThreads: $nThreads\nnumOfChunks: $numOfChunks\nuploadCompressed: $uploadCompressed\nmakeInterleavedChunks: $makeInterleavedChunks")
		println(s"minHeaderLength: $minHeaderLength\nreadLength: $readLength\nbufferSize: $bufferSize")
	}
	
	def main(args: Array[String]) 
	{
		val config = new Configuration
		config.initialize(args(0))
		setAttributes(config)
		
		val conf = new SparkConf().setAppName("Chunker")
		val sc = new SparkContext(conf)
		
		// Comment these two lines if you want to see more verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		// Testing ///////////////////////////////////////////
		//testSplitOnReadBoundary()
		//System.exit(1)
		//////////////////////////////////////////////////////
		val inputFileName = config.getFastq1Path
		val outputFolder = config.getOutputFolder
		if (config.getFastq2Path != "")
		{
			val inputFileName2 = config.getFastq2Path
			blockSize = 4
			if (makeInterleavedChunks)
				processInputFilesAndInterleave(inputFileName, inputFileName2, outputFolder)
			else
			{
				var startTime = System.currentTimeMillis
				processInputFile(inputFileName, "1", outputFolder)
				println(">> Execution time for file1: " + ((System.currentTimeMillis - startTime) / 1000) + " secs")
				
				startTime = System.currentTimeMillis
				processInputFile(inputFileName2, "2", outputFolder)
				println(">> Execution time for file2: " + ((System.currentTimeMillis - startTime) / 1000) + " secs")
			}
		}
		else
			processInputFile(inputFileName, "", outputFolder)
			
		println(">> Execution time: " + ((System.currentTimeMillis - t0) / 1000) + " secs")
	}
	
	def processInputFile(inputFileName: String, suffix: String, outputFolder: String)
	{
		val fis = new FileInputStream(new File(inputFileName))
		val gis = if (inputFileName.contains(".gz")) new GZIPInput(fis, bufferSize) else null
		val bytesRead = new Array[Int](nThreads)
		val bufferArray = new Array[ByteArray](nThreads)
		val streamMap = new scala.collection.mutable.HashMap[String, PrintWriter]()
		val startTime = System.currentTimeMillis
		
		var startIndex = 0
		var et: Long = 0
		var endReached = false
		var leftOver: ByteArray = null
		var i = 0
		while(!endReached)
		{
			val etGlobal = (System.currentTimeMillis - t0) / 1000
			val et = (System.currentTimeMillis - startTime) / 1000
			println(">> suffix: " + suffix + ", elapsed time = " + et + ", global elapsed time = " + etGlobal)
			for(index <- 0 until nThreads)
			{
				if (endReached)
				{
					bufferArray(index) = null
					bytesRead(index) = -1
					println((startIndex + index) + ". End already reached. So ignoring.")
				}
				else
				{
					val tmpBufferArray = new Array[Byte](bufferSize)
					if (gis == null)
						bytesRead(index) = fis.read(tmpBufferArray)
					else
					{
						bytesRead(index) =  gis.read(tmpBufferArray)
						println((startIndex + index) + ". gz: Bytes read = " + bytesRead(index))
					}
					if (bytesRead(index) == -1)
					{
						endReached = true
						bufferArray(index).copyFrom(leftOver)
						println((startIndex + index) + ". End reached, bufferArray.size = " + bufferArray(index).getLen)
					}
					else
					{
						// Hamid : Have to replace some of the code with Array.copy
						val readBytes = tmpBufferArray.slice(0, bytesRead(index))
						val bArray = if (leftOver == null) readBytes else (leftOver.getArray ++ readBytes)
						if (leftOver == null)
						{
							leftOver = new ByteArray(bufferSize)
							
							for(i <- 0 until nThreads)
								bufferArray(i) = new ByteArray(bufferSize*2)
						}
						splitOnReadBoundary(bArray, bufferArray(index), leftOver)
						println((startIndex + index) + ". bufferArray.size = " + bufferArray(index).getLen + ", leftOver.size = " + leftOver.getLen)
						if ((gis == null) && (bytesRead(index) < bufferSize))
						{
							println((startIndex + index) + ". Read = " + bytesRead(index) + ", bufferArray.size = " + bufferArray(index).getLen)
							endReached = true
						}
					}
				}
			}
			
			println("End reached = " + endReached)
			println(i + ". Read all " + nThreads + " chunks in the bufferArray, in " + ((System.currentTimeMillis - t0) / 1000) + " secs.")
			processChunks(bufferArray, startIndex, suffix, outputFolder, streamMap)
			println(i + ". Uploaded all " + nThreads + " chunks to " + outputFolder + " in " + ((System.currentTimeMillis - t0) / 1000) + " secs.")
			i += 1
			startIndex += nThreads
		}
		for ((k,pw) <- streamMap)
			pw.close
		if (gis != null)
			gis.close
		else
			fis.close
	}
	
	def processInputFilesAndInterleave(inputFileName1: String, inputFileName2: String, outputFolder: String)
	{
		// fastq1 ////////////////////////////////////////////////////////////
		val fis1 = new FileInputStream(new File(inputFileName1))
		val gis1 = if (inputFileName2.contains(".gz")) new GZIPInput (fis1, bufferSize) else null
		val bytesRead = new Array[Int](nThreads)
		val bArrayArray1 = new Array[Array[ByteArray]](2)
		bArrayArray1(0) = new Array[ByteArray](nThreads)
		bArrayArray1(1) = new Array[ByteArray](nThreads)
		var bufferArray1 = bArrayArray1(0)
		val bArray1 = new Array[Byte](bufferSize*2)
		var bArray1Len = 0
		var leftOver1: ByteArray = null
		// fastq2 ////////////////////////////////////////////////////////////
		val fis2 = new FileInputStream(new File(inputFileName2))
		val gis2 = if (inputFileName2.contains(".gz")) new GZIPInput (fis2, bufferSize) else null
		val bArrayArray2 = new Array[Array[ByteArray]](2)
		bArrayArray2(0) = new Array[ByteArray](nThreads)
		bArrayArray2(1) = new Array[ByteArray](nThreads)
		var bufferArray2 = bArrayArray2(0)
		val bArray2 = new Array[Byte](bufferSize*2)
		var bArray2Len = 0
		var leftOver2: ByteArray = null
		//////////////////////////////////////////////////////////////////////
		val streamMap = new scala.collection.mutable.HashMap[String, PrintWriter]()
		val startTime = System.currentTimeMillis
		
		var startIndex = 0
		var et: Long = 0
		var endReached = false
		var i = 0
		var dbi = 0
		///
		val readTime = new SWTimer
		val uploadTime = new SWTimer
		var f: scala.concurrent.Future[Unit] = null
		//
		val maxBytesAtBoundaries = 2*(readLength*2 + minHeaderLength)
		
		if (useFiles)
			new java.io.File(tmpDir).mkdirs
			
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
					val tmpBufferArray1 = new Array[Byte](bufferSize)
					val tmpBufferArray2 = new Array[Byte](bufferSize)
					if (gis1 == null)
					{
						bytesRead(index) = fis1.read(tmpBufferArray1)
						fis2.read(tmpBufferArray2)
					}
					else
					{
						bytesRead(index) =  gis1.read(tmpBufferArray1)
						gis2.read(tmpBufferArray2)
						println((startIndex + index) + ". gz: Bytes read = " + bytesRead(index))
					}
					if (bytesRead(index) == -1)
					{
						endReached = true
						bufferArray1(index).copyFrom(leftOver1.getArray, 0, leftOver1.getLen)
						bufferArray2(index).copyFrom(leftOver2.getArray, 0, leftOver2.getLen)
						println((startIndex + index) + ". End reached, bufferArray.size = " + bufferArray1(index).getLen)
					}
					else
					{
						if (leftOver1 == null)
						{
							Array.copy(tmpBufferArray1, 0, bArray1, 0, bytesRead(index))
							bArray1Len = bytesRead(index) 
							leftOver1 = new ByteArray(bufferSize)
							//
							readContent = new Array[ByteArray](nThreads)
							chunkCtr = new Array[Int](nThreads)
							if (useFiles)
							{
								gzipOutStreams = new Array[GZIPOutputStream1](nThreads)
								bytesCtr = new Array[Int](nThreads)
							}
							else
								data = new Array[ByteArray](nThreads)
							
							for(i <- 0 until nThreads)
							{
								bArrayArray1(0)(i) = new ByteArray(bufferSize*2)
								bArrayArray1(1)(i) = new ByteArray(bufferSize*2)
								//
								readContent(i) = new ByteArray(2*bufferSize*2)
								chunkCtr(i) = i
								if (useFiles)
								{
									new java.io.File(tmpDir).mkdirs
									gzipOutStreams(i) = new GZIPOutputStream1(new FileOutputStream(tmpDir + chunkCtr(i) + ".fq.gz"))
								}
								else
									data(i) = new ByteArray(chunkSize.toInt+bufferSize*2)
							}
						}
						else
						{
							Array.copy(leftOver1.getArray, 0, bArray1, 0, leftOver1.getLen)
							Array.copy(tmpBufferArray1, 0, bArray1, leftOver1.getLen, bytesRead(index))
							bArray1Len = leftOver1.getLen + bytesRead(index)
						}
						
						if (leftOver2 == null)
						{
							Array.copy(tmpBufferArray2, 0, bArray2, 0, bytesRead(index))
							bArray2Len = bytesRead(index) 
							leftOver2 = new ByteArray(bufferSize)
							
							for(i <- 0 until nThreads)
							{
								bArrayArray2(0)(i) = new ByteArray(bufferSize*2)
								bArrayArray2(1)(i) = new ByteArray(bufferSize*2)
							}
						}
						else
						{
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
			println(i + ". Read all " + nThreads + " chunks in the bufferArray, in " + ((System.currentTimeMillis - t0) / 1000) + " secs.")
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
				processInterleavedChunks(bufferArray1, bufferArray2, startIndex, outputFolder, streamMap, endReached)
			}
			//////////////////////////////////////////////////////////////////////////////////////
			uploadTime.stop
			println(i + ". Uploaded all " + nThreads + " chunks to " + outputFolder + " in " + ((System.currentTimeMillis - t0) / 1000) + " secs.")
			println(i + ". READ time = " + readTime.getSecsF + ", UPLOAD time = " + uploadTime.getSecsF)
			i += 1
			startIndex += nThreads
			dbi ^= 1
			bufferArray1 = bArrayArray1(dbi)
			bufferArray2 = bArrayArray2(dbi)
		}
		while (!f.isCompleted)
		{
			println("Future still not completed...");
			Thread.sleep(2000);
		}
		for ((k,pw) <- streamMap)
			pw.close
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
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition

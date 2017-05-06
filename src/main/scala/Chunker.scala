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
	val chunkSize: Int = 60e6.toInt
	///////////////////////////////////////////////////
	val useReadAndHeaderLenForInterleaving = true
	var chunkCtr: Array[Int] = null
	var gzipOutStreams: Array[GZIPOutputStream1] = null
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
			new PairedFastqChunker(config).makeChunks
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
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition

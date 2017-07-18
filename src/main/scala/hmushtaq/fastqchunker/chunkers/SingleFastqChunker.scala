/*
 * Copyright (C) 2017 TU Delft, The Netherlands
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Authors: Hamid Mushtaq
 *
 */
package hmushtaq.fastqchunker.chunkers

import org.apache.commons.lang3.exception.ExceptionUtils
import sys.process._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.zip.GZIPInputStream
import java.io._
import hmushtaq.fastqchunker.utils._
import java.net.URL

/**
 *
 * @author Hamid Mushtaq
 */
class SingleFastqChunker(config: Configuration)
{
	protected val nThreads = config.getNumThreads.toInt
	protected val bufferSize = config.getBlockSizeMB.toInt * 1024 * 1024
	protected val chunkSize: Int = config.getChunkSizeMB.toInt * 1024 * 1024
	protected val compLevel = config.getCompLevel.toInt
	protected val inputFileName = config.getFastq1Path
	protected val outputFolder = config.getOutputFolder
	protected val outputFolderIsLocal = config.getOutputFolderIsLocal
	protected final val MIN_ZIP_FILE_SIZE = 22

	protected val chunkCtr = new Array[Int](nThreads)
	protected val gzipOutStreams = new Array[GZIPOutputStream1](nThreads)
	protected val baFuture = new Array[ByteArray](nThreads)
	
	if (outputFolderIsLocal)
	{
		new File(outputFolder).mkdirs
		new File(outputFolder + "ulStatus").mkdirs
	}
	
	for(ti <- 0 until nThreads)
	{
		chunkCtr(ti) = ti
		gzipOutStreams(ti) = new GZIPOutputStream1(new ByteArrayOutputStream, compLevel)
	}
	
	def makeChunks()
	{	
		val inputIsURL = isURL(inputFileName)
		val fis = if (inputIsURL) new URL(inputFileName).openStream else new FileInputStream(new File(inputFileName))
		val gis = if (inputFileName.contains(".gz")) new GZIPInput(fis, bufferSize) else null
		val tmpBufferArray = new Array[Byte](bufferSize)
		val bytesRead = new Array[Int](nThreads)
		val bArray = new ByteArray(bufferSize*2)
		val bArrayArray = new Array[ByteArray](nThreads)
		var leftOver: ByteArray = null
		
		val t0 = System.currentTimeMillis
		
		for(ti <- 0 until nThreads)
		{
			baFuture(ti) = new ByteArray(bufferSize*2)
			bArrayArray(ti) = new ByteArray(bufferSize*2)
		}
		
		var startIndex = 0
		var endReached = false
		var iter = 0
		var totalBytesRead: Long = 0
		///
		val f = new Array[Future[Unit]](nThreads)
	
		println("inputIsURL: " + inputIsURL)
		while(!endReached)
		{		
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
						}
						else // Not the first ever iteration -> !(iter == 0 && index == 0)
						{
							bArray.copyFrom(leftOver)
							bArray.append(tmpBufferArray, 0, bytesRead(index))
						}
						
						splitAtReadBoundary(bArray, bArrayArray(index), leftOver)
						totalBytesRead += bArrayArray(index).getLen
					}
				}
				//////////////////////////////////////////////////////////////
				if (f(index) != null)
					Await.result(f(index), Duration.Inf)
				gzipOutStreams(index).synchronized
				{
					if (bArrayArray(index) == null)
						baFuture(index).setLen(0)
					else
						baFuture(index).copyFrom(bArrayArray(index))
				}
				f(index) = Future {
					gzipOutStreams(index).synchronized
					{
						writeChunk(index)
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
			if (gzipOutStreams(ti).getSize > MIN_ZIP_FILE_SIZE)
			{
				writeWholeBinFile(outputFolder + "/" + chunkCtr(ti) + ".fq.gz", gzipOutStreams(ti).getByteArray)
				val s = "ti: " + ti + ", " + gzipOutStreams(ti).getSize + " bytes\n"
				writeWholeFile(outputFolder + "ulStatus/" + chunkCtr(ti), s)
			}	
		}
		// Wait for the last iteration to complete
		if (gis != null)
			gis.close
		else
			fis.close
		writeWholeFile(outputFolder + "ulStatus/end.txt", "")
		println("All chunks have been uploaded!")
	}

	private def writeChunk(ti: Int)
	{
		if (baFuture(ti).getLen != 0)
		{
			gzipOutStreams(ti).write(baFuture(ti).getArray, 0, baFuture(ti).getLen)
			val numOfBytes = gzipOutStreams(ti).getSize 
			if (numOfBytes > chunkSize)
			{
				gzipOutStreams(ti).close
				
				writeWholeBinFile(outputFolder + chunkCtr(ti) + ".fq.gz", gzipOutStreams(ti).getByteArray)
				val s = "ti: " + ti + ", " + (numOfBytes / 1e6.toInt) + " MB\n"
				writeWholeFile(outputFolder + "ulStatus/" + chunkCtr(ti), s)
				
				chunkCtr(ti) += nThreads	
				gzipOutStreams(ti) = new GZIPOutputStream1(new ByteArrayOutputStream, compLevel)
			}
		}
	}
	
	protected def isURL(url: String) : Boolean =
	{
		return url.contains("ftp://") || url.contains("http://") || url.contains("https://")
	}
	
	protected def writeWholeFile(filePath: String, s: String)
	{
		if (outputFolderIsLocal)
			new PrintWriter(filePath) {write(s); close}
		else
			HDFSManager.writeWholeFile(filePath, s)
	}
	
	protected def writeWholeBinFile(filePath: String, ba: Array[Byte])
	{
		if (outputFolderIsLocal)
			new FileOutputStream(new File(filePath)) {write(ba); close}
		else
			HDFSManager.writeWholeBinFile(filePath, ba)
	}
	
	protected def splitAtReadBoundary(byteArray: ByteArray, retArray: ByteArray, leftOver: ByteArray)
	{
		val ba = byteArray.getArray
		val baSize = byteArray.getLen
		var ei = baSize-1
		var lastByte = ba(ei)
		var secLastByte = ba(ei-1)
		var thirdLastByte = ba(ei-2)
		var numOfNewLines = 0
		
		try
		{
			// Find "\n+\n" first
			while(!(lastByte == '\n' && secLastByte == '+' && thirdLastByte == '\n'))
			{
				ei -= 1
				lastByte = ba(ei)
				secLastByte = ba(ei-1)
				thirdLastByte = ba(ei-2)
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

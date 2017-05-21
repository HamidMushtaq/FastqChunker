/****************************************/
//	Program:	Chunker.scala	
//	Author:		Hamid Mushtaq  		
//	Company:	TU Delft	 	
/****************************************/
package hmushtaq.fastqchunker

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.log4j.Logger
import org.apache.log4j.Level

import hmushtaq.fastqchunker.chunkers._
import hmushtaq.fastqchunker.utils.Configuration

object Chunker
{
	def main(args: Array[String]) 
	{
		val t0 = System.currentTimeMillis
		val config = new Configuration
		config.initialize(args(0))
		
		val conf = new SparkConf().setAppName("Chunker")
		val sc = new SparkContext(conf)
		
		// Comment these two lines if you want to see more verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		if (config.getFastq2Path.trim != "")
			new PairedFastqChunker(config).makeChunks
		else
			new SingleFastqChunker(config).makeChunks
		println(">> Execution time: " + ((System.currentTimeMillis - t0) / 1000) + " secs")
	}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition

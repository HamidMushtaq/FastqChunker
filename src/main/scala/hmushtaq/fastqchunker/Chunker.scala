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
package hmushtaq.fastqchunker

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.log4j.Logger
import org.apache.log4j.Level

import hmushtaq.fastqchunker.chunkers._
import hmushtaq.fastqchunker.utils.Configuration

/**
 *
 * @author Hamid Mushtaq
 */
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

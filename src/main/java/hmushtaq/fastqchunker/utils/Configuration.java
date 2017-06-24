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
package hmushtaq.fastqchunker.utils;

import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import java.io.Serializable;
import java.lang.System;
import java.util.*;

/**
 *
 * @author Hamid Mushtaq
 */
public class Configuration implements Serializable
{
	private String fastq1Path;
	private String fastq2Path;
	private String outputFolder;
	private boolean outputFolderIsLocal;
	private String compLevel;
	private String chunkSizeMB;
	private String driverMemGB;
	private String numThreads;
	private String blockSizeMB;
	private String interleave;
	private Long startTime;
	private Document document;
	
	public void initialize(String configFile)
	{	
		try
		{
			File file = new File(configFile);
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			System.out.println("configFile = {" + configFile + "}");
			document = documentBuilder.parse(file);
			
			fastq1Path = getTagValue("fastq1Path").trim();
			fastq2Path = getTagValue("fastq2Path").trim();
			outputFolder = correctFolderName(getTagValue("outputFolder").trim());
			outputFolderIsLocal =  outputFolder.startsWith("local:");
			if (outputFolderIsLocal)
				outputFolder = outputFolder.substring(6);
			compLevel = getTagValue("compLevel").trim();
			chunkSizeMB = getTagValue("chunkSizeMB").trim(); 
			driverMemGB = getTagValue("driverMemGB").trim();
			numThreads = getTagValue("numThreads").trim(); 
			blockSizeMB = getTagValue("blockSizeMB").trim();
			if (!fastq2Path.equals("")) 
				interleave = getTagValue("interleave").trim();
			else
				interleave = "false";
			
			startTime = System.currentTimeMillis();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private String getTagValue(String tag)
	{
		return document.getElementsByTagName(tag).item(0).getTextContent();
	}
	
	private String correctFolderName(String s)
	{
		String r = s.trim();
		
		if (r.equals(""))
			return r;
		
		if (r.charAt(r.length() - 1) != '/')
			return r + '/';
		else
			return r;
	}
	
	private String getFileNameFromPath(String path)
	{
		return path.substring(path.lastIndexOf('/') + 1);
	}
	
	public String getFastq1Path()
	{
		return fastq1Path;
	}
	
	public String getFastq2Path()
	{
		return fastq2Path;
	}
	
	public String getOutputFolder()
	{
		return outputFolder;
	}
	
	public boolean getOutputFolderIsLocal()
	{
		return outputFolderIsLocal;
	}
	
	public String getCompLevel()
	{
		return compLevel;
	}
	
	public String getChunkSizeMB()
	{
		return chunkSizeMB;
	}
	
	public String getDriverMemGB()
	{
		return driverMemGB + "g";
	}
	
	public String getNumThreads()
	{
		return numThreads;
	}
	
	public String getBlockSizeMB()
	{
		return blockSizeMB;
	}
	
	public String getInterleave()
	{
		return interleave;
	}
		
	public Long getStartTime()
	{
		return startTime;
	}
	
	public void print()
	{
		System.out.println("***** Configuration *****");
		System.out.println("1. fastq1Path:\t|" + fastq1Path + "|");
		System.out.println("2. fastq2Path:\t|" + fastq2Path + "|");
		System.out.println("3. outputFolder:\t|" + outputFolder + "|");
		System.out.println("\toutputFolderIsLocal:\t|" + outputFolderIsLocal + "|");
		System.out.println("4. compLevel:\t|" + compLevel + "|");
		System.out.println("5. chunkSizeMB:\t|" + chunkSizeMB + "|");
		System.out.println("6. driverMemGB:\t|" + driverMemGB + "|");
		System.out.println("7. numThreads:\t|" + numThreads + "|");
		System.out.println("8. blockSizeMB:\t|" + blockSizeMB + "|");
		System.out.println("9. interleave:\t|" + interleave + "|");
		System.out.println("*************************");
	}
}
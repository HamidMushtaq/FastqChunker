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

import java.util.zip.GZIPInputStream;
import java.io.*;
import java.util.Arrays;

/**
 *
 * @author Hamid Mushtaq
 */
public class GZIPInput 
{
	private byte[] remBuffer;
	private byte[] readBuffer;
	private byte[] buffer;
	int bufferSize;
	int remBufferLen;
	GZIPInputStream gzi;
	InputStream fis;
	
	public GZIPInput(InputStream fis, int bufferSize) throws IOException 
	{
		gzi = new GZIPInputStream(fis, bufferSize);
		this.bufferSize = bufferSize;
		this.fis = fis;
		remBuffer = new byte[bufferSize];
		readBuffer = new byte[bufferSize];
		buffer = new byte[2*bufferSize];
		remBufferLen = 0;
	} 
	
	public int read(byte[] rBuffer)
	{
		int index = 0;
		int totalBytesRead = 0;
				
		// Copy from remBuffer first
		if (remBufferLen != 0)
		{
			System.arraycopy(remBuffer, 0, buffer, 0, remBufferLen);
			index += remBufferLen;
			totalBytesRead += remBufferLen;
		}
		
		while (index < bufferSize)
		{
			int bytesRead = -1;
			try
			{
				bytesRead = gzi.read(readBuffer);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
				System.exit(-1);
			}
			if (bytesRead == -1)
			{
				System.out.println("HAMID: Bytes read = -1");
				remBufferLen = 0;
				break;
			}
			System.arraycopy(readBuffer, 0, buffer, index, bytesRead);
			index += bytesRead;
			totalBytesRead += bytesRead;
		}
		
		if (index >= bufferSize)
		{
			System.arraycopy(buffer, 0, rBuffer, 0, bufferSize);
			remBufferLen = index - bufferSize;
			System.arraycopy(buffer, bufferSize, remBuffer, 0, remBufferLen); 
			totalBytesRead = bufferSize;
		}
		else
			System.arraycopy(buffer, 0, rBuffer, 0, totalBytesRead);
	
		return (totalBytesRead == 0)? -1 : totalBytesRead;
	}
	
	public void close()
	{
		try
		{
			gzi.close();
		}
		catch(IOException ex)
		{
			ex.printStackTrace();
		}
	}
}

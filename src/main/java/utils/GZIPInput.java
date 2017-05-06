/****************************************/
//	Class Name:	GZIPOutput	
//	Author:		Hamid Mushtaq  		
//	Company:	TU Delft	 	
/****************************************/
package utils;

import java.util.zip.GZIPInputStream;
import java.io.*;
import java.util.Arrays;

public class GZIPInput 
{
	private byte[] remBuffer;
	private byte[] readBuffer;
	private byte[] buffer;
	int bufferSize;
	GZIPInputStream gzi;
	FileInputStream fis;
	
	public GZIPInput(FileInputStream fis, int bufferSize) throws IOException 
	{
        gzi = new GZIPInputStream(fis, bufferSize);
		this.bufferSize = bufferSize;
		this.fis = fis;
		remBuffer = null;
		readBuffer = new byte[bufferSize];
		buffer = new byte[2*bufferSize]; 
    } 
	
	public int read(byte[] rBuffer)
	{
		int index = 0;
		int totalBytesRead = 0;
		
		try
		{
			// Copy from remBuffer first
			if (remBuffer != null)
			{
				System.arraycopy(remBuffer, 0, buffer, 0, remBuffer.length);
				index += remBuffer.length;
				totalBytesRead += remBuffer.length;
			}
			
			while (index < bufferSize)
			{
				int bytesRead = gzi.read(readBuffer);
				if (bytesRead == -1)
				{
					remBuffer = null;
					break;
				}
				System.arraycopy(readBuffer, 0, buffer, index, bytesRead);
				index += bytesRead;
				totalBytesRead += bytesRead;
			}
			
			if (index >= bufferSize)
			{
				System.arraycopy(buffer, 0, rBuffer, 0, bufferSize);
				remBuffer = Arrays.copyOfRange(buffer, bufferSize, index);
				totalBytesRead = bufferSize;
			}
			else
				System.arraycopy(buffer, 0, rBuffer, 0, totalBytesRead);
		}
		catch(IOException ex)
		{
			ex.printStackTrace();
			System.exit(-1);
		}
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

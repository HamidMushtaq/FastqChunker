/****************************************/
//	Class Name:	GZIPOutputStream1	
//	Author:		Hamid Mushtaq  		
//	Company:	TU Delft	 	
/****************************************/
package utils;

import java.util.zip.GZIPOutputStream;
import java.io.*;

public class GZIPOutputStream1 extends GZIPOutputStream 
{
	private ByteArrayOutputStream outputStream;
	private static final int DEFAULT_COMPRESS_LEVEL = 1;
	
	public GZIPOutputStream1(ByteArrayOutputStream out, int compressLevel) throws IOException 
	{
        super(out, true);
		def.setLevel(compressLevel);
		outputStream = out;
    } 
	
	public GZIPOutputStream1(ByteArrayOutputStream out) throws IOException 
	{
        super(out, true);
		def.setLevel(DEFAULT_COMPRESS_LEVEL);
		outputStream = out;
    }
	
	public int getSize()
	{
		return outputStream.size();
	}
	
	public byte[] getByteArray()
	{
		return outputStream.toByteArray();
	}
	
	public void reset()
	{
		outputStream.reset();
	}
}

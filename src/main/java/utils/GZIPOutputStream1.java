/****************************************/
//	Class Name:	GZIPOutputStream1	
//	Author:		Hamid Mushtaq  		
//	Company:	TU Delft	 	
/****************************************/
package utils;

import java.util.zip.GZIPOutputStream;
import java.io.OutputStream;
import java.io.IOException;

class GZIPOutputStream1 extends GZIPOutputStream 
{
	public GZIPOutputStream1(OutputStream out) throws IOException 
	{
        super(out);
		def.setLevel(1);
    } 
}
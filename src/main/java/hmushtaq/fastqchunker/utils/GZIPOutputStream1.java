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

import java.util.zip.GZIPOutputStream;
import java.io.*;

/**
 *
 * @author Hamid Mushtaq
 */
public class GZIPOutputStream1 extends GZIPOutputStream 
{
	private ByteArrayOutputStream outputStream;
	private int compressLevel;
	private static final int DEFAULT_COMPRESS_LEVEL = 1;
	
	public GZIPOutputStream1(ByteArrayOutputStream out, int compressLevel) throws IOException 
	{
        super(out, true);
		def.setLevel(compressLevel);
		outputStream = out;
		this.compressLevel = compressLevel;
    } 
	
	public GZIPOutputStream1(ByteArrayOutputStream out) throws IOException 
	{
        super(out, true);
		def.setLevel(DEFAULT_COMPRESS_LEVEL);
		outputStream = out;
		this.compressLevel = DEFAULT_COMPRESS_LEVEL;
    }
	
	public int getCompressLevel()
	{
		return compressLevel;
	}
	
	public int getSize()
	{
		return outputStream.size();
	}
	
	public ByteArrayOutputStream getOutputStream()
	{
		return outputStream;
	}
	
	public byte[] getByteArray()
	{
		return outputStream.toByteArray();
	}
}

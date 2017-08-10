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

import java.net.URL;
import java.net.URLConnection;
import java.io.*;
import javax.net.ssl.*;
import java.util.zip.GZIPInputStream;
import java.nio.charset.StandardCharsets;

// Help taken from https://stackoverflow.com/questions/41400810/gzipinputstream-closes-prematurely-when-decompressing-httpinputstream

/**
 *
 * @author Hamid Mushtaq
 */
public class URLStream
{
	//////////////////////////////////////////////////////////////////////////
	public static class AvailableInputStream extends InputStream 
	{
        private InputStream is;

        AvailableInputStream(InputStream inputstream) {
            is = inputstream;
        }

        public int read() throws IOException {
            return(is.read());
        }

        public int read(byte[] b) throws IOException {
            return(is.read(b));
        }

        public int read(byte[] b, int off, int len) throws IOException {
            return(is.read(b, off, len));
        }

        public void close() throws IOException {
            is.close();
        }

        public int available() throws IOException {
            // Always say that we have 1 more byte in the
            // buffer, even when we don't
            int a = is.available();
            if (a == 0) {
                return(1);
            } else {
                return(a);
            }
        }
    }
	//////////////////////////////////////////////////////////////////////////

	public static InputStream openHTTPsStream(String urlLink) throws Exception
	{
		URL url = new URL(urlLink);
		System.out.println("downloaded URL: "+ urlLink.toString());

        URLConnection urlConnection = url.openConnection();
        return new AvailableInputStream(urlConnection.getInputStream());
	}
	
	public static void downloadUncompressed(String urlString, String ofname) throws Exception
	{
		URL url = new URL(urlString);
		System.out.println("downloaded URL: "+ url.toString());

        // First directly wrap the HTTP inputStream with GZIPInputStream
        // and count the number of bytes we read
        // Go ahead and save the extracted stream to a file for further inspection
        int bytesFromGZIPDirect = 0;
        URLConnection urlConnection = url.openConnection();
        // Wrap the HTTPInputStream in our AvailableHttpInputStream
        AvailableInputStream ais = new AvailableInputStream(urlConnection.getInputStream());
        GZIPInputStream gzipishttp = new GZIPInputStream(ais);
        FileOutputStream directGZIPOutStream = new FileOutputStream(ofname);
        int buffersize = 1024;
        byte[] buffer = new byte[buffersize];
        int bytesRead = -1;
        while ((bytesRead = gzipishttp.read(buffer, 0, buffersize)) != -1) 
		{
            bytesFromGZIPDirect += bytesRead;
            directGZIPOutStream.write(buffer, 0, bytesRead); // save to file for further inspection
        }
        gzipishttp.close();
        directGZIPOutStream.close();
	}
}
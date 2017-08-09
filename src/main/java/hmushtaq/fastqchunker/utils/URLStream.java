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

//https://stackoverflow.com/questions/10135074/download-file-from-https-server-using-java

/**
 *
 * @author Hamid Mushtaq
 */
public class URLStream
{
	public static InputStream openHTTPsStream(String httpsURL)
	{
		try
		{
			// Create a new trust manager that trust all certificates
			TrustManager[] trustAllCerts = new TrustManager[]
			{
				new X509TrustManager() 
				{
					public java.security.cert.X509Certificate[] getAcceptedIssuers() 
					{
						return null;
					}
					public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) 
					{
					}
					public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) 
					{
					}
				}
			};
			
			SSLContext sc = SSLContext.getInstance("SSL");
			sc.init(null, trustAllCerts, new java.security.SecureRandom());
			HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
						
			URL myurl = new URL(httpsURL);
			//////////////////////////////////////////////////////////////////
			//URLConnection con = myurl.openConnection();
			HttpsURLConnection con = (HttpsURLConnection)(myurl.openConnection());
			//////////////////////////////////////////////////////////////////
			System.out.println("HAMIDJava: https!, class is " + con.getClass());
			return con.getInputStream();
		}
		catch (Exception ex) 
		{
			ex.printStackTrace();
			return null;
		}
	}

	public static void download(InputStream input, String ofname) throws IOException
	{
		byte[] buffer = new byte[8 * 1024];

		try 
		{
			OutputStream output = new FileOutputStream(ofname);
			try 
			{
				int bytesRead;
				long totalBytesRead = 0;
				int accBytes = 0;
				while ((bytesRead = input.read(buffer)) != -1) 
				{
					accBytes += bytesRead;
					totalBytesRead += bytesRead;
					if (accBytes > 1e6)
					{
						accBytes = 0;
						System.out.println((totalBytesRead / (1024*1024)) + " MB downloaded");
					}
					output.write(buffer, 0, bytesRead);
				}
			} 
			finally 
			{
				output.close();
			}
		} 
		finally 
		{
			input.close();
		}
	}
}
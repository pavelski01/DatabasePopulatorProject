package module.database_populator;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.zip.ZipException;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;

import com.ibatis.common.jdbc.ScriptRunner;

public class DatabasePopulator
{
	public static void main(String[] _args)
	{
		if (_args.length < 2)
		{
			System.out.println("Usage: DatabasePopulator [propertiesName.properties] [zipSqlScripts.zip]");
			return;
		};
		String propertiesName = _args[0];
		String zipSqlScripts = _args[1];
		Properties aProperties = new Properties();
		FileInputStream aFileInputStream = null;			
		try
		{
			aFileInputStream = new FileInputStream(propertiesName);
			aProperties.load(new BufferedInputStream(aFileInputStream));
		}
		catch (IOException ioExcp)
		{
			System.err.println("Error: Input/Output properties transfer problem!");
			return;
		}
		finally 
		{
			try { aFileInputStream.close(); } 
			catch (IOException ioExcp) 
			{
				System.err.println("Error: Input/Output properties close problem!");
				return;
			}
		}
		String dbms = aProperties.getProperty("dbms");
		String url = aProperties.getProperty("url");
		String user = aProperties.getProperty("user");
		String passwd = aProperties.getProperty("passwd");
		String driver = null;
		if (dbms.equals("mysql")) driver = "com.mysql.jdbc.Driver";
		else if (dbms.equals("postgresql")) driver = "org.postgresql.Driver";
		else
		{
			System.err.println("Error: Database Management System is not supported!");
			return;
		}
		if 
		(
			dbms == null || url == null || 
				user == null || passwd == null
		)
		{
			System.err.println("Error: Empty proprties key!");
			return;
		}		
		Driver aDriver = null;
		try 
		{
			aDriver = (Driver)Class.forName(driver).newInstance();
			DriverManager.registerDriver(aDriver);
		}
		catch (ClassNotFoundException cnfExcp)
		{
		   System.err.println("Error: Unable to load driver class!");
		   return;
		}
		catch (IllegalAccessException iaExcep) 
		{
		   System.err.println("Error: Access problem while loading!");
		   return;
		}
		catch (InstantiationException iExcp)
		{
		   System.err.println("Error: Unable to instantiate driver!");
		   return;
		} 
		catch (SQLException sqlExcp) 
		{
			System.err.println("Error: Structured Query Language exception during driver load!");
			return;
		}
		Properties info = new Properties();
		info.put("user", user);
		info.put("password", passwd);
		info.put("useUnicode", "true");
		info.put("characterEncoding", "UTF8");
		info.put("cacheServerConfiguration", "true");
		info.put("useLocalSessionState", "true");
		info.put("elideSetAutoCommits", "true");
		Connection aConnection = null;
		try 
		{ 
			aConnection = DriverManager.getConnection(url, info); 
			aConnection.setAutoCommit(false);
			aConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		} 
		catch (SQLException sqlExcp) 
		{
			System.err.println("Error: Structured Query Language exception during connection!");
			return;
		}
		ScriptRunner runner = 
			new ScriptRunner(aConnection, false, true);
		List<String> sqlScripts = new ArrayList<String>();
		String key;
		String value;
		for (int i = 0; true; i++)
		{
			key = dbms + '.' + i;
			if (aProperties.containsKey(key)) 
			{
				value = aProperties.getProperty(key);
				sqlScripts.add(value);
			}
			else break;
		}		
		org.apache.commons.compress.archivers.zip.ZipFile aZipFile = null;
		try
		{
			aZipFile = 
				new org.apache.commons.compress.archivers.zip.ZipFile(
					new File(zipSqlScripts), "UTF-8", true
				);
		}
		catch (IOException ioExcp)
		{
			System.err.println("Error: Input/Output zip transfer problem!");
			return;
		}
		Enumeration<? extends ZipArchiveEntry> entries = aZipFile.getEntries();
		ZipArchiveEntry aZipEntry;
		InputStream aInputStream;
		List<ZipArchiveEntry> zipEntries = new ArrayList<ZipArchiveEntry>();
		String zipEntryName;
        while (entries.hasMoreElements())
        {
        	aZipEntry = entries.nextElement();
        	if (aZipEntry.isDirectory()) continue;
        	zipEntryName = aZipEntry.getName();
        	for (String scriptName : sqlScripts) 
        		if (scriptName.equals(zipEntryName))
        			zipEntries.add(aZipEntry);        			
        }
        List<String> tablesToDrop = new ArrayList<String>();
        aInputStream = null;
        String readQuery;
        String firstWord;
        String lastWord;
        Scanner aScanner = null;
        boolean isSuccess = true;
        for (String ss : sqlScripts)
        {
        	if (!isSuccess) break;
        	for (ZipArchiveEntry ze : zipEntries)
        	{
        		if (!isSuccess) break;
        		zipEntryName = ze.getName();
        		if (ss.equals(zipEntryName))
        		{
        			try 
        			{ 
        				aInputStream = aZipFile.getInputStream(ze);
        				aScanner = new Scanner(aInputStream, "UTF-8");
        				readQuery = aScanner.useDelimiter("\\A").next().trim();
        				firstWord = readQuery.split(" ", 3)[0];
        				if (firstWord.equalsIgnoreCase("CREATE")) 
        				{
        					lastWord = readQuery.split(" ", 3)[2];
        					lastWord = lastWord.substring(0, lastWord.indexOf('('));
        					tablesToDrop.add("DROP TABLE " + lastWord + ";");
        				}
        				runner.runScript(new StringReader(readQuery));
    				} 
        			catch (ZipException zExcp)         			
        			{
        				isSuccess = false;
        				System.err.println("Error: Zip problem during streaming!");
					}
        			catch (UnsupportedEncodingException ueExcp)
        			{
        				isSuccess = false;
        				System.err.println("Error: Unsupported encoding!");
        			}
        			catch (IOException ioExcp)
        			{
        				isSuccess = false;
        				System.err.println("Error: Input/Output zip problem during streaming!");
					}        			
					catch (SQLException sqlExcp)
        			{
						isSuccess = false;
						System.err.println("Error: Structured Query Language exception during streaming!\n" + sqlExcp);
					}
        			finally 
        			{
        				try
        				{ 
        					aInputStream.close();
        					aScanner.close();
        				} 
        				catch (IOException ioExcp) 
        				{ System.err.println("Error: Input/Output zip problem during close stream!"); }
        			}
        		}
        	}
    	}
		if (isSuccess)
			try { aConnection.commit(); } 
			catch (SQLException sqlExcp) 
			{ System.err.println("Error: Structured Query Language exception during transaction commit!"); }
		else
		{
			List<String> errorDrop = new ArrayList<String>();
			try { aConnection.rollback(); } 
			catch (SQLException sqlExcp) 
			{ System.err.println("Error: Structured Query Language exception during transaction rollback!\n" + sqlExcp); }
			for (String ttd : tablesToDrop)
				try 
				{ runner.runScript(new StringReader(ttd));  } 
				catch (SQLException sqlExcp) 
				{ 
					System.err.println("Error: Structured Query Language exception during table drop!\n" + sqlExcp);
					errorDrop.add(ttd);
				}
				catch (IOException ioExcp)
				{ System.err.println("Error: Input/Output exception during table drop!\n" + ioExcp); }
			int index = 0;
			while (!errorDrop.isEmpty())
				try 
				{
					if (index >= errorDrop.size()) index = 0;
					runner.runScript(new StringReader(errorDrop.get(index)));
					errorDrop.remove(index);
					index++;
				} 
				catch (SQLException sqlExcp) 
				{ System.err.println("Error: Structured Query Language exception during transaction rollback!\n" + sqlExcp); }
				catch (IOException ioExcp)
				{ System.err.println("Error: Input/Output exception during transaction rollback!\n" + ioExcp); }
		}
		try { aZipFile.close(); } 
        catch (IOException ioExcp) 
        {
        	System.err.println("Error: Input/Output zip problem during close!");
			return;
		}
	}
}

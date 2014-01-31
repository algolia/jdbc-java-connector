package com.algolia.search.saas.jdbc;

/**
 * Hello world!
 *
 */
public class App 
{
	
    public static void main( String[] args )
    {
    	Settings settings = new Settings();
    	settings.parse(args);
    	if (!settings.checkArgs())
    	{
    			settings.printArgs();
    			return;
    	}
    	Worker worker = null;
    	if (settings.mode.equals("--dump"))
    		worker = new Dumper(settings);
    	else
    		worker = new Updater(settings);
    	
    	if (!worker.connect())
    	{
    		System.err.println("Unable to connect");
    		return;
    	}
    	if (worker.fetchDataBase() != 0)
    	{
    		System.err.println("Error during dumping.");
    		return;
    	}
    }
}

package SQLConnector;


public class Settings {

	public Settings()
	{
		output = "";
		target = "";
		username = "";
		password = "";
		host = "";
		query = "";
		attribute = "";
		time = "";
	}
	
	public void parse(String[] args)
	{
		for (int i  = 0; i < args.length; ++i)
		{
			String arg = args[i];
			if (arg.startsWith("-"))
			{
				if (arg.equals("--dump"))
				{
					System.out.println("Dump Mysql dataStruct to Algolia");
					mode = arg;
				}
				else if (arg.equals("--update"))
				{
					System.out.println("Update Mysql dataStruct to Algolia");
					mode = arg;
				}
				else if (arg.equals("-t") || arg.equals("--target"))
				{
					target = args[i+1];
					++i;
				}
				else if (arg.equals("-u") || arg.equals("--username"))
				{
					username = args[i+1];
					++i;
				}
				else if (arg.equals("-p") || arg.equals("--password"))
				{
					password  = args[i+1];
					++i;
				}
				else if (arg.equals("-s") || arg.equals("--source"))
				{
					host = args[i+1];
					++i;
				}
				else if (arg.equals("-q") || arg.equals("--query"))
				{
					query = args[i+1];
					++i;
				}
				else if (arg.equals("-o") || arg.equals("--output"))
				{
					output = args[i+1];
					++i;
				}
				else if (arg.equals("-a") || arg.equals("--attribute"))
				{
					attribute = args[i+1];
					++i;
				}
				else if (arg.equals("-t") || arg.equals("--time"))
				{
					time = args[i+1];
					++i;
				}
				else
				{
					System.err.println("Not yet implemented feature : " + arg);
					System.exit(1);
				}
			}
			else
			{
				System.err.println("Error on : " + arg);
				System.exit(1);	
			}
		}
	}
	
	public boolean checkArgs()
	{
		return output != "" && host != "" && target != "" && username != ""
			&& password != "";
	}
	
	public void printArgs()
	{
		System.err.print("The host : ");
		System.err.println(host);
		
		System.err.print("The output : ");
		System.err.println(output);
		
		System.err.print("The target : ");
		System.err.println(target);
		
		System.err.print("The query : ");
		System.err.println(query);
		
		System.err.print("The database username : ");
		System.err.println(username);
	}
	
	public String mode;
	public String output;
	public String host;
	public String target;
	public String username;
	public String password;
	public String query;
	public String attribute;
	public String time;
}

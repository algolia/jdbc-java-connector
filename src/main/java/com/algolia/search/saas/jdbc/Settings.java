package com.algolia.search.saas.jdbc;


public class Settings {

	public Settings() {
		output = "date.txt";
		target = "";
		username = "root";
		password = "";
		host = "";
		query = "";
		attribute = "updated_at";
		time = "1";
		mode = "dump";
		config = "";
	}
	
	public void parse(String[] args) {
		for (int i  = 0; i < args.length - 1; ++i) {
			String arg = args[i];
			if (arg.startsWith("-")) {
				if (arg.equals("-m") || arg.equals("--mode")) {
					System.out.println(args[i+1] + " Mysql dataStruct to Algolia");
					mode = args[i+1];
					++i;
				} else if (arg.equals("-t") || arg.equals("--target")) {
					target = args[i+1];
					++i;
				} else if (arg.equals("-u") || arg.equals("--username")) {
					username = args[i+1];
					++i;
				} else if (arg.equals("-p") || arg.equals("--password")) {
					password  = args[i+1];
					++i;
				} else if (arg.equals("-h") || arg.equals("--host")) {
					host = args[i+1];
					++i;
				} else if (arg.equals("-q") || arg.equals("--query")) {
					query = args[i+1];
					++i;
				} else if (arg.equals("-o") || arg.equals("--output")) {
					output = args[i+1];
					++i;
				} else if (arg.equals("-a") || arg.equals("--attribute")) {
					attribute = args[i+1];
					++i;
				} else if (arg.equals("-t") || arg.equals("--time")) {
					time = args[i+1];
					++i;
				} else if (arg.equals("-c") || arg.equals("--configuration")) {
					config = args[i+1];
					++i;
				} else {
					System.err.println("Not yet implemented feature : " + arg);
					System.exit(1);
				}
			} else {
				System.err.println("Error on : " + arg);
				System.exit(1);	
			}
		}
	}
	
	public boolean checkArgs() {
		return !host.isEmpty() && !target.isEmpty() && !username.isEmpty() && !query.isEmpty();
	}
	
	public void printArgs() {
		System.err.print("The mode : ");
		System.err.println(mode);
		
		System.err.print("The host : ");
		System.err.println(host);
		
		System.err.print("The output : ");
		System.err.println(output);
		
		System.err.print("The target : ");
		System.err.println(target);
		
		System.err.print("The config : ");
		System.err.println(config);
		
		System.err.print("The query : ");
		System.err.println(query);
		
		System.err.print("The database username : ");
		System.err.println(username);
		
		System.err.print("The attribute : ");
		System.err.println(attribute);
		
		System.err.print("The time : ");
		System.err.println(time);
	}
	
	public String mode;
	public String output;
	public String config;
	public String host;
	public String target;
	public String username;
	public String password;
	public String query;
	public String attribute;
	public String time;
}

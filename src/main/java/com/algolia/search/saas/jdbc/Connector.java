package com.algolia.search.saas.jdbc;

import java.sql.SQLException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Connector {

    private static final Options options = new Options();
    static {
        // Source
        options.addOption("s", "source", true, "JDBC connection string");
        options.getOption("source").setRequired(true);
        options.getOption("source").setArgName("jdbc:DRIVER://HOST/DB");

        options.addOption(null, "username", true, "DB username");

        options.addOption(null, "password", true, "DB password");
        
        // Destination
        options.addOption("t", "target", true, "Algolia credentials and index");
        options.getOption("target").setRequired(true);
        options.getOption("target").setArgName("APPID:APPKEY:Index");

        // Configuration
        options.addOption("c", "configuration", true, "Configuration file.");
        options.getOption("configuration").setArgName("path/to/config.json");

        // Mode
        options.addOption("d", "dump", false, "Perform a dump");
        options.addOption("u", "update", false, "Perform an update");

        // Query
        options.addOption("q", "query", true, "SQL query used to fetched all rows");
        options.getOption("query").setRequired(true);
        options.getOption("query").setArgName("SELECT * FROM table");

        // Update configuration
        options.addOption(null, "updatedAtField", true, "Field name used to find updated rows (default: updated_at)");
        options.getOption("updatedAtField").setArgName("field");

        options.addOption("r", "refresh", true, "The refresh interval, in seconds (default: 10)");
        options.getOption("refresh").setArgName("rateInMS");

        // Misc
        options.addOption("h", "help", false, "Print this help.");
    }

    private static void usage(int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(160);
        formatter.printHelp("jdbc-java-connector [option]... [path/to/config.json]", options);
        System.exit(exitCode);
    }

    public static void main(String[] args) throws ParseException, SQLException {
        CommandLine cli;

        try {
            cli = new BasicParser().parse(options, args, false);
            String[] unparsedTargets = cli.getArgs();
            // TODO
        } catch (ParseException e) {
            cli = null; // avoid non-initialized warning
            System.err.println(e.getMessage());
            usage(1);
        }

        if (cli.hasOption("help")) {
            usage(1);
        }

        Worker worker;
        try {
            if (cli.hasOption("dump")) {
                worker = new Dumper(cli);
            } else if (cli.hasOption("update")) {
                worker = new Updater(cli);
            } else {
                throw new ParseException("Either --dump or --update should be specified");
            }
        } catch (Exception e) {
            worker = null; // avoid non-initialized warning
            System.err.println(e.getMessage());
            usage(2);
        }
        try {
            // do {
            // if (!worker.fetchDataBase()) {
            // System.err.println("Error during dumping.");
            // return;
            // }
            // Thread.sleep(1000 * Integer.parseInt(settings.time));
            // } while (running);
            worker.run();
        } finally {
            if (worker != null) {
                worker.close();
            }
        }
    }
}

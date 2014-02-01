package com.algolia.search.saas.jdbc;

import java.io.FileReader;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.algolia.search.saas.AlgoliaException;

public class Connector {
    
    public static final String CONF_SOURCE = "source";
    public static final String CONF_USERNAME = "username";
    public static final String CONF_PASSWORD = "password";
    public static final String CONF_TARGET = "target";
    public static final String CONF_DUMP = "dump";
    public static final String CONF_UPDATE = "update";
    public static final String CONF_QUERY = "query";
    public static final String CONF_UPDATED_AT_FIELD = "updatedAtField";
    public static final String CONF_UNIQUE_ID_FIELD = "uniqueIDField";
    public static final String CONF_ATTRIBUTES = "attributes";
    public static final String CONF_REFRESH = "refresh";
    public static final String CONF_HELP = "heko";
    public static final String CONF_BATCH_SIZE = "batchSize";

    private static final Options options = new Options();
    static {
        // Source
        options.addOption("s", CONF_SOURCE, true, "JDBC connection string");
        options.getOption(CONF_SOURCE).setArgName("jdbc:DRIVER://HOST/DB");

        options.addOption(null, CONF_USERNAME, true, "DB username");

        options.addOption(null, CONF_PASSWORD, true, "DB password");
        
        // Destination
        options.addOption("t", CONF_TARGET, true, "Algolia credentials and index");
        options.getOption(CONF_TARGET).setArgName("APPID:APPKEY:Index");

        // Mode
        options.addOption("d", CONF_DUMP, false, "Perform a dump");
        options.addOption("u", CONF_UPDATE, false, "Perform an update");

        // Query
        options.addOption("q", CONF_QUERY, true, "SQL query used to fetched all rows");
        options.getOption(CONF_QUERY).setArgName("SELECT * FROM table");

        // Update configuration
        options.addOption(null, CONF_UPDATED_AT_FIELD, true, "Field name used to find updated rows (default: updated_at)");
        options.getOption(CONF_UPDATED_AT_FIELD).setArgName("field");
        
        // ID field
        options.addOption(null, CONF_UNIQUE_ID_FIELD, true, "Field name used to identify rows (default: id)");
        options.getOption(CONF_UNIQUE_ID_FIELD).setArgName("id");

        // Kept attributes
        options.addOption(null, CONF_ATTRIBUTES, true, "Attribute filter (default: None");
        options.getOption(CONF_ATTRIBUTES).setArgName("attributes");
        
        options.addOption("r", CONF_REFRESH, true, "The refresh interval, in seconds (default: 10)");
        options.getOption(CONF_REFRESH).setArgName("rateInMS");

        // Misc
        options.addOption("h", CONF_HELP, false, "Print this help.");
        options.addOption(null, CONF_BATCH_SIZE, true, "Size of the batch. (default: 1000)");
    }

    private static void usage(int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(160);
        formatter.printHelp("jdbc-java-connector [option]... [path/to/config.json]", options);
        System.exit(exitCode);
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws ParseException, SQLException, AlgoliaException {
        CommandLine cli;
        JSONObject configuration = null;

        try {
            cli = new BasicParser().parse(options, args, false);
            String[] unparsedTargets = cli.getArgs();
            if (unparsedTargets.length > 1) {
                usage(1);
            } else if (unparsedTargets.length == 1) {
                configuration = (JSONObject) new JSONParser().parse(new FileReader(unparsedTargets[0]));
            } else {
                configuration = new JSONObject();
            }
        } catch (Exception e) {
            cli = null; // avoid non-initialized warning
            System.err.println(e.getMessage());
            usage(1);
        }
        if (cli.hasOption(CONF_HELP)) {
            usage(1);
        }
        
        // command line arguments override the configuration
        for (Option opt : cli.getOptions()) {
            String o = opt.getLongOpt();
            if (o.equals(CONF_HELP) || o.equals(CONF_DUMP) || o.equals(CONF_UPDATE)) {
                continue;
            } else if (o.equals(CONF_ATTRIBUTES)) {
                // multi-valued attributes
                if (cli.hasOption(o)) {
                    configuration.put(o, Arrays.asList(cli.getOptionValue(o).split(",")));
                }
            } else {
                // single-valued attributes
                if (cli.hasOption(o)) {
                    configuration.put(o, cli.getOptionValue(o));
                }
            }
        }
        
        // check mandatory configuration keys
        try {
            if (!configuration.containsKey(CONF_QUERY)) {
                throw new ParseException("Missing '" + CONF_QUERY + "' option.");
            }
            if (!configuration.containsKey(CONF_SOURCE)) {
                throw new ParseException("Missing '" + CONF_SOURCE + "' option.");
            }
            if (!configuration.containsKey(CONF_TARGET)) {
                throw new ParseException("Missing '" + CONF_TARGET + "' option.");
            }
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            usage(1);
        }

        Worker worker;
        try {
            if (cli.hasOption(CONF_DUMP)) {
                worker = new Dumper(configuration);
            } else if (cli.hasOption(CONF_UPDATE)) {
                worker = new Updater(configuration);
            } else {
                throw new ParseException("Either '" + CONF_DUMP + "' or '" + CONF_UPDATE + "' options should be specified");
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

package com.algolia.search.saas.jdbc;

import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.algolia.search.saas.AlgoliaException;

public class Connector {
    
    public static final Logger LOGGER = Logger.getLogger("connector");
    
    public static final String CONF_SOURCE = "source";
    public static final String CONF_USERNAME = "username";
    public static final String CONF_PASSWORD = "password";
    public static final String CONF_TARGET = "target";
    public static final String CONF_DUMP = "dump";
    public static final String CONF_UPDATE = "update";
    public static final String CONF_QUERY = "query";
    public static final String CONF_QUERY_UPDATE = "queryUpdate";
    public static final String CONF_UPDATED_AT_FIELD = "updatedAtField";
    public static final String CONF_UNIQUE_ID_FIELD = "uniqueIDField";
    public static final String CONF_REFRESH = "refresh";
    public static final String CONF_REFRESH_DELETED = "refreshDeleted";
    public static final String CONF_HELP = "help";
    public static final String CONF_BATCH_SIZE = "batchSize";
    public static final String CONF_LOG = "log";

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
        
        options.addOption(null, CONF_QUERY_UPDATE, true, "SQL query used to fetched updated rows");
        options.getOption(CONF_QUERY_UPDATE).setArgName("SELECT * FROM table WHERE _$ < updatedAt");
        
        // ID field
        options.addOption(null, CONF_UNIQUE_ID_FIELD, true, "Field name used to identify rows (default: id)");
        options.getOption(CONF_UNIQUE_ID_FIELD).setArgName("id");
        
        options.addOption("r", CONF_REFRESH, true, "The refresh interval, in seconds (default: 10)");
        options.getOption(CONF_REFRESH).setArgName("rateInMS");
        
        options.addOption("r", CONF_REFRESH_DELETED, true, "The refresh interval to check deletion, in seconds (default: 10)");
        options.getOption(CONF_REFRESH_DELETED).setArgName("rateInM");

        // Misc
        options.addOption("h", CONF_HELP, false, "Print this help.");
        options.addOption(null, CONF_BATCH_SIZE, true, "Size of the batch. (default: 1000)");
        options.addOption(null, CONF_LOG, true, "Path to logging configuration file.");
        options.getOption(CONF_LOG).setArgName("path/to/logging.properties");
    }

    private static void usage(int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(160);
        formatter.printHelp("jdbc-java-connector [option]... [path/to/config.json]", options);
        System.exit(exitCode);
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws ParseException, SQLException, AlgoliaException {
        CommandLine cli = null;
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
            LOGGER.severe(e.getMessage());
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
            } else {
                // single-valued attributes
                if (cli.hasOption(o)) {
                    configuration.put(o, cli.getOptionValue(o));
                }
            }
        }
        
        // check mandatory configuration keys
        try {
            try {
                String loggingConfiguration = (String) configuration.get("log");
                if (loggingConfiguration != null) {
                    LogManager.getLogManager().readConfiguration(new FileInputStream(loggingConfiguration));
                }
            } catch (Exception e) {
                throw new ParseException("Cannot log to " + (String) configuration.get("log"));
            }            
            if (!configuration.containsKey(CONF_QUERY)) {
                throw new ParseException("Missing '" + CONF_QUERY + "' option.");
            }
            if (!configuration.containsKey(CONF_SOURCE)) {
                throw new ParseException("Missing '" + CONF_SOURCE + "' option.");
            }
            if (!configuration.containsKey(CONF_TARGET)) {
                throw new ParseException("Missing '" + CONF_TARGET + "' option.");
            }
            if (cli.hasOption(CONF_UPDATE)) {
            	if (!configuration.containsKey(CONF_QUERY_UPDATE)) {
            		throw new ParseException("Missing '" + CONF_QUERY_UPDATE + "' option.");
            	}
            	if (!configuration.containsKey(CONF_UPDATED_AT_FIELD)) {
            		throw new ParseException("Missing '" + CONF_UPDATED_AT_FIELD + "' option.");
            	}
            }
            if (!configuration.containsKey(CONF_UNIQUE_ID_FIELD)) {
            	throw new ParseException("Missing '" + CONF_UNIQUE_ID_FIELD + "' option.");
            }
        } catch (ParseException e) {
            LOGGER.severe(e.getMessage());
            usage(1);
        }
        
        LOGGER.info("* Starting connector");

        Worker updateWorker = null;
        Worker deleteWorker = null;
        try {
            if (cli.hasOption(CONF_DUMP)) {
            	Worker worker = new Dumper(configuration);
                worker.run();
                return;
            } else if (cli.hasOption(CONF_UPDATE)) {
                updateWorker = new Updater(configuration);
                deleteWorker = new Deleter(configuration);
            } else {
                throw new ParseException("Either '" + CONF_DUMP + "' or '" + CONF_UPDATE + "' options should be specified");
            }
        } catch (Exception e) {
        	LOGGER.severe(e.getMessage());
            usage(2);
        }
        try {
        	long timeBetweenUpdate = Long.parseLong(configuration.get(CONF_REFRESH) != null ? (String) configuration.get(CONF_REFRESH) : "0");
        	long timeBetweenDelete = Long.parseLong(configuration.get(CONF_REFRESH_DELETED) != null ? (String) configuration.get(CONF_REFRESH_DELETED) : "0");
        	long elapsedLoop = 0;
            try {
            	if (!updateWorker.isInitialised())
            		(new Dumper(configuration)).run();
            	do {
            		if (timeBetweenDelete != 0 && timeBetweenDelete <= elapsedLoop) {
            			deleteWorker.run();
            			elapsedLoop = 0;
            		}
            		updateWorker.run();
            		Thread.sleep(1000 * timeBetweenUpdate);
            		elapsedLoop += timeBetweenUpdate;
            	} while(timeBetweenUpdate != 0);
			} catch (JSONException e) {
				LOGGER.severe(e.getMessage());
			} catch (InterruptedException e) {
				LOGGER.severe(e.getMessage());
			}
        } finally {
            if (updateWorker != null) {
            	updateWorker.close();
            }
            if (deleteWorker != null) {
            	deleteWorker.close();
            }
        }
    }
}

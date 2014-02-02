package com.algolia.search.saas.jdbc;

import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.logging.Level;
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
    public static final String CONF_APPLICATION_ID = "applicationId";
    public static final String CONF_API_KEY = "apiKey";
    public static final String CONF_INDEX = "index";
    public static final String CONF_DUMP_ONLY = "dump";
    public static final String CONF_SELECT_QUERY = "selectQuery";
    public static final String CONF_UPDATE_QUERY = "updateQuery";
    public static final String CONF_UPDATED_AT_FIELD = "updatedAtField";
    public static final String CONF_PRIMARY_FIELD = "primaryField";
    public static final String CONF_REFRESH_RATE = "refreshRate";
    public static final String CONF_DELETE_RATE = "deleteRate";
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
        options.addOption(null, CONF_APPLICATION_ID, true, "Algolia APPLICATION_ID");
        options.getOption(CONF_APPLICATION_ID).setArgName("YourApplicationID");
        options.addOption(null, CONF_API_KEY, true, "Algolia APPI_KEY");
        options.getOption(CONF_API_KEY).setArgName("YourApiKey");
        options.addOption(null, CONF_INDEX, true, "Destination index");
        options.getOption(CONF_INDEX).setArgName("YourIndex");

        // Mode
        options.addOption("d", CONF_DUMP_ONLY, false, "Perform a dump only");

        // Query
        options.addOption("q", CONF_SELECT_QUERY, true, "SQL query used to fetched all rows");
        options.getOption(CONF_SELECT_QUERY).setArgName("SELECT * FROM table");
        options.addOption("u", CONF_UPDATE_QUERY, true, "SQL query used to fetched updated rows. Use _$ as placeholder");
        options.getOption(CONF_UPDATE_QUERY).setArgName("SELECT * FROM table WHERE _$ > updatedAt");

        // Update configuration
        options.addOption(null, CONF_UPDATED_AT_FIELD, true, "Field name used to find updated rows (default: updated_at)");
        options.getOption(CONF_UPDATED_AT_FIELD).setArgName("field");
        
        
        // ID field
        options.addOption(null, CONF_PRIMARY_FIELD, true, "Field name used to identify rows (default: id)");
        options.getOption(CONF_PRIMARY_FIELD).setArgName("id");
        
        options.addOption("r", CONF_REFRESH_RATE, true, "The refresh interval, in seconds (default: 10)");
        options.getOption(CONF_REFRESH_RATE).setArgName("rateInMS");

        options.addOption("r", CONF_DELETE_RATE, true, "The refresh interval to check deletion, in minutes (default: 10)");
        options.getOption(CONF_DELETE_RATE).setArgName("rateInM");

        // Misc
        options.addOption("h", CONF_HELP, false, "Print this help.");
        options.addOption(null, CONF_BATCH_SIZE, true, "Size of the batch. (default: 1000)");
        options.addOption(null, CONF_LOG, true, "Path to logging configuration file.");
        options.getOption(CONF_LOG).setArgName("path/to/logging.properties");
        
        // force drivers loading
        try {
            Class.forName("com.mysql.jdbc.Driver"); 
            Class.forName("org.postgresql.Driver");
            Class.forName("org.sqlite.JDBC");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void usage(int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(160);
        formatter.printHelp("jdbc-java-connector [option]... [path/to/config.json]", options);
        System.exit(exitCode);
    }
    
    private static void tryUntil(Worker worker, long sleepTime) {
    	while (true) {
    			try {
					worker.run();
					return;
				} catch (SQLException | AlgoliaException | JSONException e) {
				    LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
    			try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					continue;
				}
    	}
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws SQLException, ParseException {
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
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            usage(1);
        }
        if (cli.hasOption(CONF_HELP)) {
            usage(1);
        }
        
        // command line arguments override the configuration
        for (Option opt : cli.getOptions()) {
            String o = opt.getLongOpt();
            if (o.equals(CONF_HELP) || o.equals(CONF_DUMP_ONLY)) {
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
            if (!configuration.containsKey(CONF_SELECT_QUERY)) {
                throw new ParseException("Missing '" + CONF_SELECT_QUERY + "' option.");
            }
            if (!configuration.containsKey(CONF_SOURCE)) {
                throw new ParseException("Missing '" + CONF_SOURCE + "' option.");
            }
            if (!configuration.containsKey(CONF_APPLICATION_ID)) {
                throw new ParseException("Missing '" + CONF_APPLICATION_ID + "' option.");
            }
            if (!configuration.containsKey(CONF_API_KEY)) {
                throw new ParseException("Missing '" + CONF_API_KEY + "' option.");
            }
            if (!configuration.containsKey(CONF_INDEX)) {
                throw new ParseException("Missing '" + CONF_INDEX + "' option.");
            }
            if (!configuration.containsKey(CONF_PRIMARY_FIELD)) {
                throw new ParseException("Missing '" + CONF_PRIMARY_FIELD + "' option.");
            }
            if (!cli.hasOption(CONF_DUMP_ONLY)) {
            	if (!configuration.containsKey(CONF_UPDATE_QUERY)) {
            		throw new ParseException("Missing '" + CONF_UPDATE_QUERY + "' option.");
            	}
            	if (!configuration.containsKey(CONF_UPDATED_AT_FIELD)) {
            		throw new ParseException("Missing '" + CONF_UPDATED_AT_FIELD + "' option.");
            	}
            }
        } catch (ParseException e) {
            LOGGER.severe(e.getMessage());
            usage(1);
        }
        
        LOGGER.info("* Starting connector");

        Worker updateWorker = null;
        Worker deleteWorker = null;
        try {
            if (cli.hasOption(CONF_DUMP_ONLY)) {
            	tryUntil(new Dumper(configuration), 1000);
                return;
            } else {
                updateWorker = new Updater(configuration);
                deleteWorker = new Deleter(configuration);
            }
        } catch (Exception e) {
        	LOGGER.log(Level.SEVERE, e.getMessage(), e);
            usage(2);
        }
        try {
        	long timeBetweenUpdate = Long.parseLong(configuration.get(CONF_REFRESH_RATE) != null ? (String) configuration.get(CONF_REFRESH_RATE) : "10");
        	long timeBetweenDelete = Long.parseLong(configuration.get(CONF_DELETE_RATE) != null ? (String) configuration.get(CONF_DELETE_RATE) : "10");
        	long elapsedLoop = 0;
            try {
            	if (!updateWorker.isInitialised())
            		tryUntil(new Dumper(configuration), 1000); // Constructor can throw
            	do {
            		if (timeBetweenDelete != 0 && timeBetweenDelete <= elapsedLoop) {
            			tryUntil(deleteWorker, 1000);
            			elapsedLoop = 0;
            		}
            		tryUntil(updateWorker, 1000);
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

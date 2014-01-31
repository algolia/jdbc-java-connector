package com.algolia.search.saas.jdbc;

import java.io.FileReader;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.algolia.search.saas.APIClient;
import com.algolia.search.saas.Index;

public abstract class Worker {
    protected final CommandLine cli;

    public Worker(CommandLine cli) throws SQLException, org.apache.commons.cli.ParseException, JSONException {
        this.cli = cli;

     // Load configuration
        String configuration = cli.getOptionValue("configuration", null);
        if (configuration != null) {
            JSONParser parser = new JSONParser();
            try {
                this.configuration = (JSONObject) parser.parse(new FileReader(configuration));
            } catch (Exception e) {
                throw new org.apache.commons.cli.ParseException("Failed to parse configuration file:" + e.getMessage());
            }
        } else {
            this.configuration = null;
        }
        
        final String confTarget = this.configuration != null && this.configuration.containsKey("target") ? this.configuration.get("target").toString() : null;
        final String confSource = this.configuration != null && this.configuration.containsKey("source") ? this.configuration.get("source").toString() : null;
        final String confUsername = this.configuration != null && this.configuration.containsKey("username") ? this.configuration.get("username").toString() : null;
        final String confPassword = this.configuration != null && this.configuration.containsKey("password") ? this.configuration.get("password").toString() : null;
        final List<String> confAttributes = this.configuration != null && this.configuration.containsKey("attributes") ? Arrays.asList(((JSONArray)this.configuration.get("attributes")).join(",").split(",")) : null;
        
        // Algolia connection
        final String target = cli.getOptionValue("target", confTarget);
        if (target == null) {
            throw new ParseException("Missing --target option");
        }
        String[] APPInfo = target.split(":");
        if (APPInfo.length != 3) {
            throw new org.apache.commons.cli.ParseException("Invalid target: " + target);
        }
        this.client = new APIClient(APPInfo[0], APPInfo[1]);
        this.index = this.client.initIndex(APPInfo[2]);
        
        // JDBC connection
        final String source = cli.getOptionValue("source", confSource);
        if (source == null) {
            throw new ParseException("Missing --source option");
        }
        this.database = DriverManager.getConnection(source, cli.getOptionValue("username", confUsername), cli.getOptionValue("password", confPassword));

        this.attributes = cli.getOptionValues("attributes") != null ? Arrays.asList(cli.getOptionValues("attributes")) : confAttributes;
    }
    
    public void close() throws SQLException {
        if (this.database != null) {
            this.database.close();
        }
    }
    
    public abstract void run() throws ParseException, SQLException;

    protected final List<String> attributes;
    protected final JSONObject configuration;
    protected final APIClient client;
    protected final Index index;
    protected final java.sql.Connection database;
}

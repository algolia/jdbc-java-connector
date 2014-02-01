package com.algolia.search.saas.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.commons.cli.ParseException;
import org.json.JSONException;
import org.json.simple.JSONObject;

import com.algolia.search.saas.APIClient;
import com.algolia.search.saas.AlgoliaException;
import com.algolia.search.saas.Index;

public abstract class Worker {
    public Worker(JSONObject configuration) throws SQLException, org.apache.commons.cli.ParseException, JSONException {
        this.configuration = configuration;

        // Algolia connection
        final String target = (String) configuration.get(Connector.CONF_TARGET);
        if (target == null) {
            throw new ParseException("Missing '" + Connector.CONF_TARGET + "' option");
        }
        String[] APPInfo = target.split(":");
        if (APPInfo.length != 3) {
            throw new org.apache.commons.cli.ParseException("Invalid target: " + target);
        }
        this.client = new APIClient(APPInfo[0], APPInfo[1]);
        this.index = this.client.initIndex(APPInfo[2]);
        
        // JDBC connection
        final String source = (String) configuration.get(Connector.CONF_SOURCE);
        if (source == null) {
            throw new ParseException("Missing '" + Connector.CONF_SOURCE + "' option");
        }
        this.database = DriverManager.getConnection(source, (String) configuration.get(Connector.CONF_USERNAME), (String) configuration.get(Connector.CONF_PASSWORD));
    }
    
    public void close() throws SQLException {
        if (this.database != null) {
            this.database.close();
        }
    }
    
    public abstract void run() throws SQLException, AlgoliaException;

    protected final JSONObject configuration;
    protected final APIClient client;
    protected final Index index;
    protected final java.sql.Connection database;
}

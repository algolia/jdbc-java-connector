package com.algolia.search.saas.jdbc;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
        final String applicationId = (String) configuration.get(Connector.CONF_APPLICATION_ID);
        assert(applicationId != null);
        final String apiKey = (String) configuration.get(Connector.CONF_API_KEY);
        assert(apiKey != null);
        final String index = (String) configuration.get(Connector.CONF_INDEX);
        assert(index != null);
        this.client = new APIClient(applicationId, apiKey);
        this.index = this.client.initIndex(index);

        // DB configuration
        this.idField = (String) configuration.get(Connector.CONF_PRIMARY_FIELD);
        assert (idField != null);
        
        // JDBC connection
        final String source = (String) configuration.get(Connector.CONF_SOURCE);
        if (source == null) {
            throw new ParseException("Missing '" + Connector.CONF_SOURCE + "' option");
        }
        this.database = DriverManager.getConnection(source, (String) configuration.get(Connector.CONF_USERNAME), (String) configuration.get(Connector.CONF_PASSWORD));
        
        // Batch configuration
        Object batchSize = this.configuration.get(Connector.CONF_BATCH_SIZE);
        this.batchSize = batchSize == null ? 1000 : (batchSize instanceof Integer || batchSize instanceof Long ? (Long) batchSize : Long.parseLong((String) batchSize));
    }
    
    public void close() throws SQLException {
        if (this.database != null) {
            this.database.close();
        }
    }
    
    public boolean isInitialised() {
    	try {
    		org.json.JSONObject settings = this.index.getSettings();
			return settings.getJSONObject("userData").getString("lastUpdatedAt") != null;
		} catch (Exception e) { //Index not found or First dump
			Connector.LOGGER.info(e.getMessage());
			return false;
		}
    }
    
	public List<org.json.JSONObject> addSetting(List<org.json.JSONObject> actions, String lastUpdate) throws JSONException {
    	org.json.JSONObject action = new org.json.JSONObject();
    	org.json.JSONObject userData = new org.json.JSONObject();
    	org.json.JSONObject lastUpdatedAt = new org.json.JSONObject();
    	lastUpdatedAt.put("lastUpdatedAt", lastUpdate);
    	userData.put("userData", lastUpdatedAt);
    	action.put("action", "changeSettings");
    	action.put("body", userData);
    	actions.add(action);
    	
    	return actions;
    }
	
	protected void iterateOnQuery(java.sql.PreparedStatement stmt) throws SQLException, JSONException, AlgoliaException {
		Connector.LOGGER.info("  Enumerating database");
		String lastUpdatedAt = "0";
		ResultSet rs = stmt.executeQuery();
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columns = rsmd.getColumnCount();
            List<org.json.JSONObject> actions = new ArrayList<org.json.JSONObject>();
            while (rs.next()) {
                org.json.JSONObject obj = new org.json.JSONObject();
                for (int i = 1; i < columns + 1; i++) {
                    try {
                    	if (rsmd.getColumnName(i).equals(idField)) {
                    		obj.put("objectID", rs.getObject(i));
                    	} else {
                    		obj.put(rsmd.getColumnName(i), rs.getObject(i));
                    		 if (rsmd.getColumnName(i).equals(configuration.get(Connector.CONF_UPDATED_AT_FIELD))
                    				 && rs.getString(i).compareTo(lastUpdatedAt) > 0) {
                         		lastUpdatedAt = rs.getString(i);
                    		 }
                    	}
                    } catch (JSONException e) {
                        throw new Error(e);
                    }
                }
                org.json.JSONObject action = new org.json.JSONObject();
                action.put("action", "addObject");
                action.put("body",obj);
                actions.add(action);
                if (actions.size() >= batchSize) {
                	actions = addSetting(actions, lastUpdatedAt);
                    this.index.batch(actions);
                    actions.clear();
                }
            }
            if (!actions.isEmpty()) {
            	actions = addSetting(actions, lastUpdatedAt);
                this.index.batch(actions);
            }
        } finally {
            rs.close();
        }
        Connector.LOGGER.info("  Database enumerated");
	}
    
    public abstract void run() throws SQLException, AlgoliaException, JSONException;

    protected final JSONObject configuration;
    protected final APIClient client;
    protected final Index index;
    protected final java.sql.Connection database;
    protected final long batchSize;
    protected final String idField;
}

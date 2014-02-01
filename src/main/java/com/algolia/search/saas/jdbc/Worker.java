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
        // DB configuration
        this.idField = (String) configuration.get(Connector.CONF_UNIQUE_ID_FIELD);
        assert (idField != null);
        
        // JDBC connection
        final String source = (String) configuration.get(Connector.CONF_SOURCE);
        if (source == null) {
            throw new ParseException("Missing '" + Connector.CONF_SOURCE + "' option");
        }
        this.database = DriverManager.getConnection(source, (String) configuration.get(Connector.CONF_USERNAME), (String) configuration.get(Connector.CONF_PASSWORD));
        
        // Batch configuration
        String batchSize = (String) this.configuration.get(Connector.CONF_BATCH_SIZE);
        this.batchSize = batchSize == null ? 1000 : Integer.parseInt(batchSize);
    }
    
    public void close() throws SQLException {
        if (this.database != null) {
            this.database.close();
        }
    }
    
	public List<org.json.JSONObject> addSetting(List<org.json.JSONObject> actions, String lastUpdate) throws JSONException {
    	org.json.JSONObject action = new org.json.JSONObject();
    	org.json.JSONObject userData = new org.json.JSONObject();
    	userData.put("lastUpdatedAt", lastUpdate);
    	action.put("userData", userData);
    	action.put("action", "changeSettings");
    	action.put("body", userData);
    	actions.add(action);
    	
    	return actions;
    }
	
	protected void iterateOnQuery(java.sql.PreparedStatement stmt) throws SQLException, JSONException, AlgoliaException {
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
                	actions = addSetting(actions, lastUpdatedAt); //TODO
                    //this.index.batch(actions);// TODO
                    actions.clear();
                }
            }
            if (!actions.isEmpty()) {
                //this.index.batch(actions); //TODO
            }
        } finally {
            rs.close();
        }
	}
    
    public abstract void run() throws SQLException, AlgoliaException, JSONException;

    protected final JSONObject configuration;
    protected final APIClient client;
    protected final Index index;
    protected final java.sql.Connection database;
    protected final int batchSize;
    protected final String idField;
}

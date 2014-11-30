package com.algolia.search.saas.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.JSONException;
import org.json.simple.JSONObject;

import com.algolia.search.saas.AlgoliaException;

public class Updater extends Worker {
    private static final String LAST_UPDATED_AT_SETTING = "lastUpdatedAt"; 
    
    public Updater(JSONObject configuration) throws SQLException, org.apache.commons.cli.ParseException, JSONException {
        super(configuration);
    }
    
    private long getTimeOfColumn(ResultSetMetaData rsmd, ResultSet rs, int column) throws SQLException {
        switch (rsmd.getColumnType(column)) {
        case Types.DATE:
            return rs.getDate(column).getTime() / 1000;
        case Types.TIME:
            return rs.getTime(column).getTime() / 1000;
        case Types.TIMESTAMP:
            return rs.getTimestamp(column).getTime() / 1000;
        default:
            throw new Error("Unknow column type");
        }
    }
    
    @Override
    protected void onRow(ResultSetMetaData rsmd, ResultSet rs, String objectID, org.json.JSONObject obj) throws SQLException {
        for (int i = 1; i < rsmd.getColumnCount() + 1; i++) {
            if (rsmd.getColumnLabel(i).equals(configuration.get(Connector.CONF_UPDATED_AT_FIELD))) {
                long t = getTimeOfColumn(rsmd, rs, i);
                if (t > this.lastUpdatedAt) {
                    this.lastUpdatedAt = t;
                }
            }
        }
        org.json.JSONObject action = new org.json.JSONObject();
        try {
			action.put("action", "addObject");
			action.put("body", obj);
		} catch (JSONException e) {
			throw new Error(e);
		}
        actions.add(action);
    }
    
    @Override
    protected void fillUserData(org.json.JSONObject userData) throws JSONException {
        userData.put(LAST_UPDATED_AT_SETTING, this.lastUpdatedAt);
    }
    
    @Override
    public void run() throws SQLException, JSONException, AlgoliaException {
    	checkAndReconnect();
        SimpleDateFormat df = new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    	try {
			this.lastUpdatedAt = this.userData.getLong(LAST_UPDATED_AT_SETTING);
		} catch (Exception e) {
			// No index or uninitialized settings
		    this.lastUpdatedAt = 0; 
		}
        String query = (String) configuration.get(Connector.CONF_UPDATE_QUERY);
        assert (query != null);
    	Connector.LOGGER.info("Start updating job");
    	iterateOnQuery(query.replaceAll("_\\$", "'" +  df.format(new Date(lastUpdatedAt * 1000)) + "'"));
    	Connector.LOGGER.info("Updating job done");
    }
    private long lastUpdatedAt;
}

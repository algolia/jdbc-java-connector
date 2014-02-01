package com.algolia.search.saas.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.json.JSONException;
import org.json.simple.JSONObject;

import com.algolia.search.saas.AlgoliaException;

public class Updater extends Worker {
    public Updater(JSONObject configuration) throws SQLException, org.apache.commons.cli.ParseException, JSONException {
        super(configuration);
        this.lastUpdatedAt = "0";
        this.query = (String) configuration.get(Connector.CONF_QUERY);
        assert (query != null);
    }
    
    @Override
    public void run() throws SQLException, JSONException, AlgoliaException {
    	try {
			org.json.JSONObject settings = index.getSettings();
			lastUpdatedAt = settings.getJSONObject("userDate").getString("lastUpdatedAt");
		} catch (Exception e) {
			//No index or uninitialized settings 
		}
    	String sql = query.replaceAll("_\\$", lastUpdatedAt);
    	final java.sql.PreparedStatement stmt = database.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        if (stmt instanceof com.mysql.jdbc.Statement) {
            ((com.mysql.jdbc.Statement) stmt).enableStreamingResults();
        }
        //TODO Activate on the other jdbc
    	
    	
    	iterateOnQuery(stmt);
    }

//    public boolean fetchDataBase() throws SQLException, AlgoliaException, JSONException {
//        List<org.json.JSONObject> json = null;
//        JSONArray attributes = null;
//        if (configuration_ != null && configuration_.get("attributes") != null)
//            attributes = (JSONArray)configuration_.get("attributes");
//        
//        String sql = settings_.query.replaceAll("_\\$", currentTime_);
//        SQLQuery query = dataBase_.listTableContent(sql);
//        
//        if (configuration_ != null && configuration_.get("track") != null)
//            query.trackAttribute((String)configuration_.get("track"));
//        
//        while (!(json = query.toJson(1000, attributes)).isEmpty()) {
//            index_.addObjects(json);
//        }
//        currentTime_ = query.lastUpdate;
//        return true;
//    }
    
    private String lastUpdatedAt;
    private final String query;
}

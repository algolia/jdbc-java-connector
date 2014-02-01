package com.algolia.search.saas.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.ParseException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.simple.JSONObject;

import com.algolia.search.saas.AlgoliaException;

public class Deleter extends Worker {

	public Deleter(JSONObject configuration) throws SQLException,
			ParseException, JSONException {
		super(configuration);
		String batchSize = (String) this.configuration.get(Connector.CONF_BATCH_SIZE);
        this.batchSize = batchSize == null ? 1000 : Integer.parseInt(batchSize);
        this.query = (String) configuration.get(Connector.CONF_QUERY);
        assert (query != null);
        this.idField = (String) configuration.get(Connector.CONF_UNIQUE_ID_FIELD);
        assert (idField != null);
        this.stmt = database.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        if (stmt instanceof com.mysql.jdbc.Statement) {
            ((com.mysql.jdbc.Statement) stmt).enableStreamingResults();
        }
	}

	@Override
	public void run() throws SQLException, AlgoliaException, JSONException {
		Set<String> ids = new HashSet<String>();
		int nbPages = 0;
		int i = 0;
		do {
			org.json.JSONObject elements = null;//index.browse(i); TODO
			nbPages = elements.getInt("nbPages");
			JSONArray hits = elements.getJSONArray("hits");
			for (int j = 0; j < elements.getInt("nbHits"); ++j) {
				ids.add(hits.getString(j));
			}
		} while (i < nbPages);
		
		ResultSet rs = stmt.executeQuery();
		try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columns = rsmd.getColumnCount();
            while (rs.next()) {
                for (i = 1; i < columns + 1; i++) {
                    try {
                    	if (rsmd.getColumnName(i).equals(idField)) {
                    		ids.remove(rs.getObject(i));
                    	}
                    } catch (JSONException e) {
                        throw new Error(e);
                    }
                }
            }
            List<org.json.JSONObject> actions = new ArrayList<org.json.JSONObject>();
            
            for (String id : ids) {
            	org.json.JSONObject action = new org.json.JSONObject();
            	org.json.JSONObject body = new org.json.JSONObject();
            	body.put("ObjectID", id);
            	action.put("action", "deleteObject");
                action.put("body",body);
                actions.add(action);
                if (actions.size() >= batchSize) {
                    //this.index.batch(actions); TODO
                    actions.clear();
                }
            }
            if (!actions.isEmpty()) {
                this.index.addObjects(actions);
            }
        } finally {
            rs.close();
        }
	}
	
	private final java.sql.PreparedStatement stmt;
    private final String query;
    private final String idField;
    private final int batchSize;

}

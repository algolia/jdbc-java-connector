package com.algolia.search.saas.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
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
	}
	
	@Override
	protected void onRow(ResultSetMetaData rsmd, ResultSet rs, String objectID, org.json.JSONObject obj) throws SQLException, AlgoliaException {
	    if (ids.remove(objectID)) {
	        return;
	    }
	    try {
            org.json.JSONObject action = new org.json.JSONObject();
            action.put("action", "addObject");
            action.put("body",obj);
            actions.add(action);
	    } catch (JSONException e) {
	        throw new Error(e);
	    }
	}

	@Override
	protected void fillUserData(org.json.JSONObject userData) throws JSONException {
	}

	@Override
	public void run() throws SQLException, AlgoliaException, JSONException {
		Connector.LOGGER.info("Start deleting job");
		Connector.LOGGER.info("  Enumerating remote index");
		int nbPages = 0;
		int i = 0;
		ids.clear();
		do {
			org.json.JSONObject elements = index.browse(i, 1000);
			nbPages = elements.getInt("nbPages");
			JSONArray hits = elements.getJSONArray("hits");
			for (int j = 0; j < hits.length(); ++j) {
				ids.add(hits.getJSONObject(j).getString("objectID"));
			}
			++i;
		} while (i < nbPages);
		Connector.LOGGER.info("  Remote index enumerated (" + ids.size() + " hits found)");

		Connector.LOGGER.info("  Enumerate database");
		Connector.LOGGER.info("  Add missing elements");
		iterateOnQuery((String) configuration.get(Connector.CONF_SELECT_QUERY));
		Connector.LOGGER.info("  Remove deleted elements");
		for (String id : ids) {
			try {
	            org.json.JSONObject action = new org.json.JSONObject();
	            action.put("action", "deleteObject");
	            action.put("objectID", id);
	            actions.add(action);
	            if (actions.size() >= batchSize) {
                    push(actions);
                    actions.clear();
                }
		    } catch (JSONException e) {
		        throw new Error(e);
		    }
		}
		push(actions);
		Connector.LOGGER.info("  Database enumerated");
		Connector.LOGGER.info("Deleting job done");
	}
	
    private final Set<String> ids = new HashSet<String>();
}

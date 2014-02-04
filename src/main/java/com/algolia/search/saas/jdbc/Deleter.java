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
	}
	
	@Override
	protected void onRow(ResultSetMetaData rsmd, ResultSet rs, String objectID) throws SQLException, AlgoliaException {
	    if (ids.contains(objectID)) {
	        return;
	    }
	    try {
            org.json.JSONObject action = new org.json.JSONObject();
            action.put("action", "deleteObject");
            action.put("objectID", objectID);
            actions.add(action);
	    } catch (JSONException e) {
	        throw new Error(e);
	    }
        if (actions.size() >= batchSize) {
            push(actions);
            actions.clear();
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
			System.out.println(elements);
			JSONArray hits = elements.getJSONArray("hits");
			for (int j = 0; j < hits.length(); ++j) {
				ids.add(hits.getJSONObject(j).getString("objectID"));
			}
			++i;
		} while (i < nbPages);
		Connector.LOGGER.info("  Remote index enumerated (" + ids.size() + " hits found)");

		Connector.LOGGER.info("  Enumerate database");
		iterateOnQuery((String) configuration.get(Connector.CONF_SELECT_QUERY));
		push(actions);
		Connector.LOGGER.info("  Database enumerated");
		Connector.LOGGER.info("Deleting job done");
	}
	
    private final Set<String> ids = new HashSet<String>();
	private final List<org.json.JSONObject> actions = new ArrayList<>();
}

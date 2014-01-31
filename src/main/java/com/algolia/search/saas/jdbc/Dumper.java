package com.algolia.search.saas.jdbc;

import java.sql.SQLException;
import java.util.List;
import org.json.simple.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.algolia.search.saas.APIClient;
import com.algolia.search.saas.AlgoliaException;
import com.algolia.search.saas.Index;


public class Dumper extends Worker {

	public Dumper(Settings settings) {
		settings_ = settings;
		dataBase_ = null;
		client_ = null;
		index_ = null;
	}
	
	public boolean connect() throws SQLException {
		String[] APPInfo = settings_.target.split(":");
		if (APPInfo.length != 3)
			return false;
		client_  = new APIClient(APPInfo[0], APPInfo[1]);
		index_ = client_.initIndex(APPInfo[2]);
		
		dataBase_ = new Connector(settings_.host, settings_.username, settings_.password);	
		return dataBase_.connect();
	}
	
	public boolean fetchDataBase() throws SQLException, AlgoliaException, JSONException {
		List<JSONObject> json = null;
		SQLQuery query = dataBase_.listTableContent(settings_.query);
		while (!(json = query.toJson(1000, (JSONArray)configuration_.get("attributes"))).isEmpty()) {
			index_.addObjects(json);
		}
		return true;
	}
	
	private APIClient client_;
	private Index index_;
	private Connector dataBase_;
	private Settings settings_;
}

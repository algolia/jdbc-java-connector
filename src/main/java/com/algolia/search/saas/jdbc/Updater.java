package com.algolia.search.saas.jdbc;

import java.sql.SQLException;
import java.util.List;

import org.json.JSONException;
import org.json.simple.JSONArray;
import com.algolia.search.saas.APIClient;
import com.algolia.search.saas.AlgoliaException;
import com.algolia.search.saas.Index;

public class Updater extends Worker {
	public Updater(Settings settings) {
		settings_ = settings;
		dataBase_ = null;
		client_ = null;
		index_ = null;
		currentTime_ = "";
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
		List<org.json.JSONObject> json = null;
		JSONArray attributes = null;
		if (configuration_ != null && configuration_.get("attributes") != null)
			attributes = (JSONArray)configuration_.get("attributes");
		
		String sql = settings_.query.replaceAll("_\\$", currentTime_);
		SQLQuery query = dataBase_.listTableContent(sql);
		
		if (configuration_ != null && configuration_.get("track") != null)
			query.trackAttribute((String)configuration_.get("track"));
		
		while (!(json = query.toJson(1000, attributes)).isEmpty()) {
			index_.addObjects(json);
		}
		currentTime_ = query.lastUpdate;
		return true;
	}
	
	private String currentTime_;
	private APIClient client_;
	private Index index_;
	private Connector dataBase_;
	private Settings settings_;
}

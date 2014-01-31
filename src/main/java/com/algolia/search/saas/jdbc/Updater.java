package com.algolia.search.saas.jdbc;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.algolia.search.saas.APIClient;
import com.algolia.search.saas.AlgoliaException;
import com.algolia.search.saas.Index;

public class Updater extends Worker {
	public Updater(Settings settings)
	{
		settings_ = settings;
		dataBase_ = null;
	}
	
	public boolean parseConfig(String fileName)
	{
		JSONParser parser = new JSONParser();
		
		try {
			configuration_ = (JSONObject)parser.parse(new FileReader(fileName));
		} catch (FileNotFoundException e) {
			return false; //TODO
		} catch (IOException e) {
			return false; //TODO
		} catch (ParseException e) {
			return false; //TODO 
		}
		return true;
	}
	
	public boolean connect()
	{
		String[] APPInfo = settings_.target.split(":");
		if (APPInfo.length != 3)
			return false;
		client_  = new APIClient(APPInfo[0], APPInfo[1]);
		index_ = client_.initIndex(APPInfo[2]);
		
		dataBase_ = new Connector(settings_.host, settings_.username, settings_.password);	
		return dataBase_.connect() == 0;
	}
	
	public Integer fetchDataBase()
	{
		List<org.json.JSONObject> json = null;
		
		String sql = settings_.query.replaceAll("_\\$", currentTime_);
		SQLQuery query = dataBase_.listTableContent(sql);
		query.trackAttribute((String)configuration_.get("track"));
		try {
			while (!(json = query.toJson(1000, (JSONArray)configuration_.get("attributes"))).isEmpty())
			{
				index_.addObjects(json);
			}
		} catch (SQLException e) {
			return 1;
		} catch (AlgoliaException e) {
			return 1;
		}
		currentTime_ = query.lastUpdate;
		return 0;
	}
	
	private String currentTime_;
	private APIClient client_;
	private Index index_;
	private Connector dataBase_;
	private Settings settings_;
}

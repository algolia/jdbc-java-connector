package com.algolia.search.saas.jdbc;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

import org.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.algolia.search.saas.AlgoliaException;

public abstract class Worker {
	public abstract boolean connect() throws SQLException;
	public boolean fetchDataBase() throws SQLException, AlgoliaException, JSONException {
		return false;
	}
public boolean parseConfig(String fileName) throws IOException, ParseException
	{
		JSONParser parser = new JSONParser();
		
		try {
			configuration_ = (JSONObject)parser.parse(new FileReader(fileName));
		} catch (FileNotFoundException e) {
			configuration_ = new JSONObject();
			return false;
		}
		return true;
	}
	
	protected JSONObject configuration_;
}

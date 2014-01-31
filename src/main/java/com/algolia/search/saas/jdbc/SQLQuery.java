package com.algolia.search.saas.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.simple.JSONArray;

public class SQLQuery {
	public SQLQuery(ResultSet query)
	{
		query_ = query;
		trackedAttribute_ = "";
		lastUpdate = "";
	}
	
	public void trackAttribute(String attr)
	{
		trackedAttribute_ = attr;
	}
	
	public List<JSONObject> toJson(int nb, JSONArray attributes) throws SQLException
	{
		Integer i = 0;
		List<JSONObject> res = new ArrayList<JSONObject>();
		
		if (query_.last())
			return null;
			
		
		while (i != nb && query_.next())
		{
			org.json.JSONObject jsonObject = new org.json.JSONObject();
			for (int j = 0; j < nbColumn(); ++j)
			{
				try {
					if (attributes.contains(query_.getMetaData().getColumnClassName(j)))
						jsonObject.put(query_.getMetaData().getColumnName(j), query_.getObject(j).toString());
					if (trackedAttribute_ != "" && trackedAttribute_.equals(query_.getMetaData().getColumnClassName(j))
							&& lastUpdate.compareTo(query_.getObject(j).toString()) > 0)
						lastUpdate = query_.getObject(j).toString();
				} catch (SQLException e) {
					System.err.println("Warning"); //TODO msg
				} catch (JSONException e) {
					System.err.println("Warning"); //TODO msg
				}
				++i;
			}
			res.add(jsonObject);
		}
		return res;
	}
	
	public boolean next(int i)
	{
		try {
			return query_.next();
		} catch (SQLException e) {
			return false;
		}
	}
	
	public boolean prev(int i)
	{
		try {
			return query_.previous();
		} catch (SQLException e) {
			return false;
		}
	}
	
	public int nbColumn()
	{
		try {
			return query_.getMetaData().getColumnCount();
		} catch (SQLException e) {
			return -1;
		}
	}
	
	public String lastUpdate;
	private ResultSet query_;
	private String trackedAttribute_;
}

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
	
	public List<JSONObject> toJson(int nb, JSONArray attributes) throws SQLException, JSONException
	{
		Integer i = 0;
		List<JSONObject> res = new ArrayList<JSONObject>();
		
		if (query_.last())
			return res;
			
		
		while (i != nb && query_.next())
		{
			org.json.JSONObject jsonObject = new org.json.JSONObject();
			for (int j = 0; j < nbColumn(); ++j)
			{
					if (attributes == null || attributes.contains(query_.getMetaData().getColumnClassName(j)))
						jsonObject.put(query_.getMetaData().getColumnName(j), query_.getObject(j).toString());
					if (trackedAttribute_ != "" && trackedAttribute_.equals(query_.getMetaData().getColumnClassName(j))
							&& lastUpdate.compareTo(query_.getObject(j).toString()) > 0)
						lastUpdate = query_.getObject(j).toString();
				++i;
			}
			res.add(jsonObject);
		}
		return res;
	}
	
	public boolean next(int i) throws SQLException
	{
		return query_.next();
	}
	
	public boolean prev(int i) throws SQLException
	{
		return query_.previous();
	}
	
	public int nbColumn() throws SQLException
	{
		return query_.getMetaData().getColumnCount();
	}
	
	public String lastUpdate;
	private ResultSet query_;
	private String trackedAttribute_;
}

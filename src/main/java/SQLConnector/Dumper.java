package SQLConnector;

import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.json.simple.JSONArray;
import org.json.JSONObject;

import com.algolia.search.saas.APIClient;
import com.algolia.search.saas.AlgoliaException;
import com.algolia.search.saas.Index;


public class Dumper extends Worker {

	public Dumper(Settings settings)
	{
		settings_ = settings;
		dataBase_ = null;
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
		Vector<String> tablesName = dataBase_.listTableName();
		List<JSONObject> json = null;
		
		for (String tableName : tablesName)
		{
			SQLQuery query = dataBase_.listTableContent(tableName);
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
		}
		return 0;
	}
	
	private APIClient client_;
	private Index index_;
	private Connector dataBase_;
	private Settings settings_;
}

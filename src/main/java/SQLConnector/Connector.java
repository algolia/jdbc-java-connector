package SQLConnector;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;



public class Connector {
	
	public Connector(String url, String username, String password)
	{
		url_ = url;
		username_ = username;
		password_ = password;
		database_ = null;
	}

	public Integer connect()
	{
		try {
			database_ = DriverManager.getConnection(url_, username_, password_);
		} catch (SQLException e) {
			return 1;
		}
		return 0;
	}
	
	public Boolean isConnected()
	{
		try {
			return database_ != null && !database_.isClosed();
		} catch (SQLException e) {
			return false;
		}
	}
	
	public Integer close()
	{
		try {
			database_.close();
		} catch (SQLException e) {
			return 1;
		}
		return 0;
	}
	
	public SQLQuery listTableContent(String sql)
	{	
		try { 
			return new SQLQuery(database_.prepareStatement(sql).executeQuery());
		} catch (SQLException e) {
			return null;
		}
	}
	
	public Vector<String> listTableName()
	{
		Vector<String> tablesName = new Vector<String>();
		
		ResultSet req;
		try {
			req = database_.getMetaData().getTables(null, null, "%", null);
		} catch (SQLException e) {
			return null;
		}
		try {
			while (req.next()) {
				tablesName.add(req.getString(3));
			}
		} catch (SQLException e) {
			return null;
		}
		return tablesName;
	}
	
	private java.sql.Connection database_;
	private String username_;
	private String password_;
	private String url_;
}

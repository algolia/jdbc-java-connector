package com.algolia.search.saas.jdbc;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import org.json.JSONException;
import org.json.simple.parser.ParseException;

import com.algolia.search.saas.AlgoliaException;

public class Connector {

	public static void main(String[] args) throws SQLException,
			AlgoliaException, JSONException, IOException, ParseException, NumberFormatException, InterruptedException {
		boolean running = false; //TODO DEV
		
		Settings settings = new Settings();
		settings.parse(args);
		if (!settings.checkArgs()) {
			settings.printArgs();
			return;
		}
		settings.printArgs();
		Worker worker = null;
		if (settings.mode.equals("dump"))
			worker = new Dumper(settings);
		else
			worker = new Updater(settings);

		if (!worker.connect()) {
			System.err.println("Unable to connect");
			return;
		}
		worker.parseConfig(settings.config);
		do {
			if (!worker.fetchDataBase()) {
				System.err.println("Error during dumping.");
				return;
			}
			Thread.sleep(1000 * Integer.parseInt(settings.time));
		} while (running);
		
	}

	public Connector(String url, String username, String password) {
		url_ = url;
		username_ = username;
		password_ = password;
		database_ = null;
	}

	public boolean connect() throws SQLException {
		database_ = DriverManager.getConnection(url_, username_, password_);
		return true;
	}

	public Boolean isConnected() {
		try {
			return database_ != null && !database_.isClosed();
		} catch (SQLException e) {
			return false;
		}
	}

	public boolean close() throws SQLException {
		database_.close();
		return true;
	}

	public SQLQuery listTableContent(String sql) throws SQLException {
		return new SQLQuery(database_.prepareStatement(sql).executeQuery());
	}

	public Vector<String> listTableName() throws SQLException {
		Vector<String> tablesName = new Vector<String>();

		ResultSet req;
		req = database_.getMetaData().getTables(null, null, "%", null);
		while (req.next()) {
			tablesName.add(req.getString(3));
		}
		return tablesName;
	}

	private java.sql.Connection database_;
	private String username_;
	private String password_;
	private String url_;
}

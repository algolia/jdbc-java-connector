package SQLConnector;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public abstract class Worker {
	public abstract boolean connect();
	public abstract Integer fetchDataBase();
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
	
	protected JSONObject configuration_;
}

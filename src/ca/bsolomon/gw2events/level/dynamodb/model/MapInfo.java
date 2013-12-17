package ca.bsolomon.gw2events.level.dynamodb.model;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName="MapInfo")
public class MapInfo {

	private int mapId;
	private int lowLevelRange;
	private int highLevelRange;
	
	@DynamoDBHashKey(attributeName="MapId")  
	public int getMapId() {
		return mapId;
	}
	public void setMapId(int mapId) {
		this.mapId = mapId;
	}
	
	@DynamoDBAttribute(attributeName="LowLevelRange")  
	public int getLowLevelRange() {
		return lowLevelRange;
	}
	public void setLowLevelRange(int lowLevelRange) {
		this.lowLevelRange = lowLevelRange;
	}
	
	@DynamoDBAttribute(attributeName="HighLevelRange")  
	public int getHighLevelRange() {
		return highLevelRange;
	}
	public void setHighLevelRange(int highLevelRange) {
		this.highLevelRange = highLevelRange;
	}
	@Override
	public String toString() {
		return "MapInfo [mapId=" + mapId + ", lowLevelRange=" + lowLevelRange
				+ ", highLevelRange=" + highLevelRange + "]";
	}
	
}

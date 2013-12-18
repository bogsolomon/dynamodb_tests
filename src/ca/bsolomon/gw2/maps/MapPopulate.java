package ca.bsolomon.gw2.maps;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ca.bsolomon.gw2event.api.GW2EventsAPI;
import ca.bsolomon.gw2event.api.dao.MapDetail;
import ca.bsolomon.gw2events.level.dynamodb.model.MapInfo;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper.FailedBatch;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

public class MapPopulate {
	
	private AmazonDynamoDBClient dynamoDB = null;
	private DynamoDBMapper mapper = null;
	
	private String tableName = "MapInfo";
	
	private void initDynamoDbClient() throws Exception {
		dynamoDB = new AmazonDynamoDBClient(new ClasspathPropertiesFileCredentialsProvider());
        Region region = Region.getRegion(Regions.US_EAST_1);
        dynamoDB.setRegion(region);
        
        mapper = new DynamoDBMapper(dynamoDB);
    }
	
	public static void main(String[] args) {
		MapPopulate populate = new MapPopulate();
		try {
			populate.initDynamoDbClient();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		populate.deleteTable();
		
		if (!populate.tableExists()) {
			populate.createTable();
		}
		
		GW2EventsAPI gw2Api = new GW2EventsAPI();
		Map<String, MapDetail> maps = gw2Api.queryMapDetails().getMaps().get(0);
		
		List<MapInfo> mapsToAdd = new ArrayList<>();
		
		for (String mapId:maps.keySet()) {
			Integer mapIdInt = Integer.parseInt(mapId);
			
			if (populate.getMapById(mapIdInt) == null) {
				MapInfo info = new MapInfo();
				info.setMapId(mapIdInt);
				info.setLowLevelRange(maps.get(mapId).getMinLevel());
				info.setHighLevelRange(maps.get(mapId).getMaxLevel());
				
				mapsToAdd.add(info);
				
				if (mapsToAdd.size() == 25) {
					populate.batchAddMaps(mapsToAdd);
					
					mapsToAdd.clear();
				}
			}
		}
		
		populate.batchAddMaps(mapsToAdd);
		
		System.out.println("New data inserted");
		
		long startTime = System.currentTimeMillis();
		
		List<MapInfo> data = populate.getAllMaps();
		
		long endTime = System.currentTimeMillis();
		
		System.out.println("First query: "+(endTime - startTime));
		
		for (MapInfo mapInfo:data) {
			System.out.println(mapInfo);
		}
		
		System.out.println("------------------------------------------------------");
		
		startTime = System.currentTimeMillis();
		
		data = populate.getMapRange(0, 80);
		
		endTime = System.currentTimeMillis();
		
		System.out.println("Second query: "+(endTime - startTime));
		
		for (MapInfo mapInfo:data) {
			System.out.println(mapInfo);
		}
		
		System.out.println("------------------------------------------------------");
		
		startTime = System.currentTimeMillis();
		
		MapInfo info = populate.getMapById(22);
		
		endTime = System.currentTimeMillis();
		
		System.out.println("Single query: "+(endTime - startTime));
		
		System.out.println(info);
	}

	private void addMap(MapInfo info) {
		mapper.save(info);
	}
	
	private void batchAddMaps(List<MapInfo> infos) {
		List<FailedBatch> failed = mapper.batchSave(infos);
		
		if (failed.size() > 0) {
			System.out.println(failed.size()+" items failed to save.");
			System.out.println(failed.get(0).getException());
		}
	}

	private void deleteTable() {
		DeleteTableRequest deleteReq = new DeleteTableRequest(tableName);
		dynamoDB.deleteTable(deleteReq);
		
		waitForTableToBecomeDeleted(tableName);
	}

	private void generateData(String folderName) {
		Path folderPath = Paths.get(folderName);
		
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(folderPath)) {
			for (Path file: stream) {
				parseMapFile(file);
			}
		} catch (IOException | DirectoryIteratorException x) {
			System.out.println("File path: "+folderPath+" does not exist or is not directory.");
		}
	}

	private void parseMapFile(Path file) throws IOException {
		List<String> fileLines = Files.readAllLines(file, Charset.defaultCharset());
		
		if (fileLines!=null && fileLines.size() == 2) {
			String mapId = fileLines.get(0);
			String levelRange = fileLines.get(1);
			
			String[] levelRanges = levelRange.split("\\|");
			
			MapInfo map = new MapInfo();
			map.setMapId(Integer.parseInt(mapId));
			map.setLowLevelRange(Integer.parseInt(levelRanges[0]));
			map.setHighLevelRange(Integer.parseInt(levelRanges[1]));
			
	        mapper.save(map);
		}
	}

	private boolean tableExists() {
		try {
            DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
            TableDescription tableDescription = dynamoDB.describeTable(request).getTable();
            String tableStatus = tableDescription.getTableStatus();
            System.out.println("  - current state: " + tableStatus);
            if (tableStatus.equals(TableStatus.ACTIVE.toString())) return true;
        } catch (AmazonServiceException ase) {
            if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) throw ase;
        }
		
		return false;
	}

	private void createTable() {
		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
            .withKeySchema(new KeySchemaElement().withAttributeName("MapId").withKeyType(KeyType.HASH))
            .withAttributeDefinitions(new AttributeDefinition().withAttributeName("MapId").withAttributeType(ScalarAttributeType.N))
            .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
        TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
        System.out.println("Created Table: " + createdTableDescription);
        
        waitForTableToBecomeAvailable(tableName);
	}

	private void waitForTableToBecomeAvailable(String tableName) {
        System.out.println("Waiting for " + tableName + " to become ACTIVE...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {Thread.sleep(1000 * 20);} catch (Exception e) {}
            try {
                DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
                TableDescription tableDescription = dynamoDB.describeTable(request).getTable();
                String tableStatus = tableDescription.getTableStatus();
                System.out.println("  - current state: " + tableStatus);
                if (tableStatus.equals(TableStatus.ACTIVE.toString())) return;
            } catch (AmazonServiceException ase) {
                if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) throw ase;
            }
        }

        throw new RuntimeException("Table " + tableName + " never went active");
    }
	
	private void waitForTableToBecomeDeleted(String tableName) {
		System.out.println("Waiting for " + tableName + " to become DELETED...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {Thread.sleep(1000 * 20);} catch (Exception e) {}
            try {
                DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
                TableDescription tableDescription = dynamoDB.describeTable(request).getTable();
                String tableStatus = tableDescription.getTableStatus();
                System.out.println("  - current state: " + tableStatus);
                if (!tableStatus.equals(TableStatus.DELETING.toString())) return;
            } catch (AmazonServiceException ase) {
                if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == true) {
                	System.out.println("  - current state: DELETED");
                	return;
                }
            }
        }

        throw new RuntimeException("Table " + tableName + " never was deleted");
	}
	
	private List<MapInfo> getAllMaps() {
		List<MapInfo> mapInfo = new ArrayList<>();
		
		DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
		
		PaginatedScanList<MapInfo> result = mapper.scan(MapInfo.class, scanExpression);
		
		for (MapInfo info:result) {
			mapInfo.add(info);
		}
		
		return mapInfo;
	}
	
	private List<MapInfo> getMapRange(Integer lowRange, Integer highRange) {
		List<MapInfo> mapInfo = new ArrayList<>();
		
		DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
		scanExpression.addFilterCondition("LowLevelRange", new Condition().withComparisonOperator(ComparisonOperator.GE).withAttributeValueList(
                new AttributeValue().withN(lowRange.toString())));
		scanExpression.addFilterCondition("HighLevelRange", new Condition().withComparisonOperator(ComparisonOperator.LE).withAttributeValueList(
                new AttributeValue().withN(highRange.toString())));
		
		PaginatedScanList<MapInfo> result = mapper.scan(MapInfo.class, scanExpression);
		
		for (MapInfo info:result) {
			mapInfo.add(info);
		}
		
		return mapInfo;
	}
	
	private MapInfo getMapById(int mapId) {
		return mapper.load(MapInfo.class, mapId);
	}
}

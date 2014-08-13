package examples;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class Inserter {
	
	
	// for reading from file
	
	private static BufferedReader br;
	private static List<String> rAsn;
	private static List<String> rPrefix;
	private static List<String> rVal;
	private static List<String> rRir;
	private static List<String> rVrp;
	private static List<String> rIpver;
	private static List<String> rBinary;
	
	private static List<String> vId;
	private static List<String> vAsn;
	private static List<String> vPrefix;
	private static List<String> vMax;
	private static List<String> vMin;
	private static List<String> vBinary;
	
	
	// for inserting

	public static String path = "/home/skims/Downloads/neo4j-community-2.0.1/data/graph.db2";
	
	public static void insert() {

		@SuppressWarnings("deprecation")
		org.neo4j.kernel.DefaultFileSystemAbstraction fileSystem = new org.neo4j.kernel.DefaultFileSystemAbstraction();

		// START SNIPPET: insert
		BatchInserter inserter = BatchInserters.inserter(path, fileSystem);

		System.out.println(inserter.getStoreDir());

		// label for node
		Label routeLabel = DynamicLabel.label("Route");
		
		// route node properties
		inserter.createDeferredSchemaIndex(routeLabel).on("asn").create();
		inserter.createDeferredSchemaIndex(routeLabel).on("prefix").create();
		inserter.createDeferredSchemaIndex(routeLabel).on("validity").create();
		inserter.createDeferredSchemaIndex(routeLabel).on("rir").create();
		inserter.createDeferredSchemaIndex(routeLabel).on("ipver").create();
		inserter.createDeferredSchemaIndex(routeLabel).on("binary").create();
		
		
		// ######### Routes #########
		
		Map<String, Object> propertiesRoute = new HashMap<>();
		
		System.out.println("Inserting routes data");
		
		long time = System.currentTimeMillis();
		
		long allTime = System.currentTimeMillis();
		
		int count = rAsn.size();
//		count = 10;
		
		for(int i=0; i<count; i++) {
			propertiesRoute.put("asn",rAsn.get(i));
			propertiesRoute.put("prefix",rPrefix.get(i));
			propertiesRoute.put("validity",rVal.get(i));
			propertiesRoute.put("rir",rRir.get(i));
			propertiesRoute.put("ipver",rIpver.get(i));
			propertiesRoute.put("binary",rBinary.get(i));
			
			// insert the current node
			long nodeId = inserter.createNode(propertiesRoute, routeLabel);
//			System.out.println("node: " + nodeId);
		}
		
		time = System.currentTimeMillis() - time;
		
		System.out.println("Duration routes: " + time + " ms");
		
		System.out.println("finished routes");
		
		
		// ######### VRP #########

		Label vrpLabel = DynamicLabel.label("Vrp");

		// vrp node properties
//		inserter.createDeferredSchemaIndex(vrpLabel).on("id").create();
		inserter.createDeferredSchemaIndex(vrpLabel).on("asn").create();
		inserter.createDeferredSchemaIndex(vrpLabel).on("ip").create();
		inserter.createDeferredSchemaIndex(vrpLabel).on("max").create();
		inserter.createDeferredSchemaIndex(vrpLabel).on("min").create();
		inserter.createDeferredSchemaIndex(vrpLabel).on("binary").create();
		
		Map<String, Object> propertiesVrp = new HashMap<>();
		
		System.out.println("Inserting VRP data");
		
		time = System.currentTimeMillis();
		
		count = vAsn.size();
//		count = 10;
		
		for(int i=0; i<count; i++) {
//			propertiesVrp.put("id",vId.get(i));
			propertiesVrp.put("asn",vAsn.get(i));
			propertiesVrp.put("ip",vPrefix.get(i));
			propertiesVrp.put("max",vMax.get(i));
			propertiesVrp.put("min",vMin.get(i));
			propertiesVrp.put("binary",vBinary.get(i));
			
			// insert the current node
			long nodeId = inserter.createNode(propertiesVrp, vrpLabel);
//			System.out.println("node: " + nodeId);
		}
		
		time = System.currentTimeMillis() - time;
		
		System.out.println("Duration routes: " + time + " ms");
		
		System.out.println("finished VRP");
		
		
		
		// ######### Inserter shutdown #########
		
		time = System.currentTimeMillis();
		
		inserter.shutdown();
		
		time = System.currentTimeMillis() - time;
		
		System.out.println("Duration inserter shutdown: " + time + "ms");
		
		System.out.println("inserter shutdown");
		
		allTime = System.currentTimeMillis() - allTime;
		
		System.out.println("Duration for all operations: " + allTime + "ms");
		


	}
	
	private static void readFile(String file) {
		try {
			br = new BufferedReader(new FileReader(file));
			rAsn = new ArrayList<String>();
			rPrefix = new ArrayList<String>();
			rVal = new ArrayList<String>();
			rRir = new ArrayList<String>();
			rVrp = new ArrayList<String>();
			rIpver = new ArrayList<String>();
			rBinary = new ArrayList<String>();
			
			vId = new ArrayList<String>();
			vAsn = new ArrayList<String>();
			vPrefix = new ArrayList<String>();
			vMax = new ArrayList<String>();
			vMin = new ArrayList<String>();
			vBinary = new ArrayList<String>();
			
			boolean route = true;
			
			String line = br.readLine();
			
			System.out.println("start reading");
			
			while(line != null) {
				
				if(line.contains("ROUTES")) {
					route = true;
				}
				else if(line.contains("VRP")) {
					route = false;
				}
				else {
					String[] content = line.split(";");
					
					if(route) {
						rAsn.add(content[0]);
						rPrefix.add(content[1]);
						rVal.add(content[2]);
						rRir.add(content[3]);
						rVrp.add(content[4]);
						rIpver.add(content[5]);
						rBinary.add(content[6]);
					}
					else {
						vId.add(content[0]);
						vAsn.add(content[1]);
						vPrefix.add(content[2]);
						vMax.add(content[3]);
						vMin.add(content[4]);
						vBinary.add(content[5]);
					}
					
				}				
				
				line = br.readLine();
			}
			
			System.out.println("finished");
			System.out.println("R Lists sizes: " + rAsn.size());
			System.out.println("V Lists sizes: " + vId.size());
			
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]) {
		
		String file = "queries";
		
		System.out.println("##### Read File #####");
		
		long time = System.currentTimeMillis();
				
		readFile(file);
		
		time = System.currentTimeMillis() - time;
		
		System.out.println("Duration: " + time + " ms");
		
		System.out.println("\n\n##### Neo4j #####");
		
		insert();
	}
}

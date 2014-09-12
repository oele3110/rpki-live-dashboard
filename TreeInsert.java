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
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class TreeInsert {

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
	
	private static Tree tree;
	
//	public static String path = "E:\\Daten\\Documents\\Neo4j\\default.graphdb";
	public static String path = "/home/skims/Downloads/neo4j-community-2.0.1/data/graph2.db";

	public static void main(String[] args) {
		
		
		String file = "queries";

		System.out.println("##### Read File #####");

		long time = System.currentTimeMillis();

		readFile(file);

		time = System.currentTimeMillis() - time;

		System.out.println("Duration: " + time + " ms");

		// checkVrpOccurance();
		
		
		tree = new Tree();
		/*
		tree.insertNode(new ip1Node(23, 5));
		
		if(tree.hasNode(23))
			System.out.println("YES");
		else
			System.out.println("NO");
		
		if(tree.hasNode(5))
			System.out.println("YES");
		else
			System.out.println("NO");
		
		
		ip1Node n = tree.getNode(23);
		System.out.println(n.id);
		
		n.setId(42);
		n = tree.getNode(23);
		System.out.println(n.id);
		*/
		
		System.out.println("##### Inserting route nodes into tree #####");
		
		time = System.currentTimeMillis();
		
		listToTree(tree);
		
		time = System.currentTimeMillis() - time;

		System.out.println("Duration: " + time + " ms");
				
//		printTree(tree);
		
		System.out.println("Number of nodes (inclusive root node): " + countNodes(tree));
		
		insertInDb();
		
		
	}
	
	private static void insertInDb() {
		
		
		long time = 0;
		long allTime = 0;
		
		long rootId = 0;
		
		BatchInserter inserter = null;
		
		@SuppressWarnings("deprecation")
		org.neo4j.kernel.DefaultFileSystemAbstraction fileSystem = new org.neo4j.kernel.DefaultFileSystemAbstraction();
		
		// START SNIPPET: insert
		inserter = BatchInserters.inserter(path, fileSystem);

		// ######### Prefix #########
		
		Label rootLabel = DynamicLabel.label("Root");
		
		inserter.createDeferredSchemaIndex(rootLabel).on("root").create();
		
		Label ip1Label = DynamicLabel.label("Ip1");
		
		inserter.createDeferredSchemaIndex(ip1Label).on("ip").create();
		
		Label ip2Label = DynamicLabel.label("Ip2");
		
		inserter.createDeferredSchemaIndex(ip2Label).on("ip").create();
		
		Label ip3Label = DynamicLabel.label("Ip3");
		
		inserter.createDeferredSchemaIndex(ip3Label).on("ip").create();
		
		Label ip4Label = DynamicLabel.label("Ip4");
		
		inserter.createDeferredSchemaIndex(ip4Label).on("ip").create();
		
		Label lengthLabel = DynamicLabel.label("Length");
		
		inserter.createDeferredSchemaIndex(lengthLabel).on("len").create();
		
		// ######### Route #########
		
		Label routeLabel = DynamicLabel.label("Route");
		
		// route node properties
		inserter.createDeferredSchemaIndex(routeLabel).on("asn").create();
//		inserter.createDeferredSchemaIndex(routeLabel).on("prefix").create();
		inserter.createDeferredSchemaIndex(routeLabel).on("validity").create();
		inserter.createDeferredSchemaIndex(routeLabel).on("rir").create();
		inserter.createDeferredSchemaIndex(routeLabel).on("ipver").create();
		inserter.createDeferredSchemaIndex(routeLabel).on("binary").create();
		
		// ######### VRP #########
		
		Label vrpLabel = DynamicLabel.label("Vrp");
				
		// vrp node properties
//		inserter.createDeferredSchemaIndex(vrpLabel).on("id").create();
		inserter.createDeferredSchemaIndex(vrpLabel).on("asn").create();
		inserter.createDeferredSchemaIndex(vrpLabel).on("ip").create();
		inserter.createDeferredSchemaIndex(vrpLabel).on("max").create();
		inserter.createDeferredSchemaIndex(vrpLabel).on("min").create();
		inserter.createDeferredSchemaIndex(vrpLabel).on("binary").create();
		
		// ######### Relationship #########
		
		RelationshipType ip1Rel = DynamicRelationshipType.withName("ip_1");
		RelationshipType ip2Rel = DynamicRelationshipType.withName("ip_2");
		RelationshipType ip3Rel = DynamicRelationshipType.withName("ip_3");
		RelationshipType ip4Rel = DynamicRelationshipType.withName("ip_4");
		RelationshipType lengthRel = DynamicRelationshipType.withName("len_rel");
		RelationshipType routeData = DynamicRelationshipType.withName("route_data");
		RelationshipType hasVrp = DynamicRelationshipType.withName("has_vrp");
		
		
		try {
		
			// ######### VRP insert #########
			
			Map<String, Object> propertiesVrp = new HashMap<>();
						
			System.out.println("Inserting VRP data");
			
			allTime = System.currentTimeMillis();
			
			time = System.currentTimeMillis();
			
			int count = vAsn.size();
			// count = 10;
	
			Integer[] idArray = new Integer[count + 1];
				
			
			Map<String, Object> propertiesRoot = new HashMap<>();
			
			propertiesRoot.put("root", "root");
			rootId = inserter.createNode(propertiesRoot, rootLabel);
			

			for (int i = 0; i < count; i++) {
				// propertiesVrp.put("id",vId.get(i));
				propertiesVrp.put("asn", vAsn.get(i));
				propertiesVrp.put("ip", vPrefix.get(i));
				propertiesVrp.put("max", vMax.get(i));
				propertiesVrp.put("min", vMin.get(i));
				propertiesVrp.put("binary", vBinary.get(i));

				// insert the current node
				long nodeId = inserter.createNode(propertiesVrp, vrpLabel);
				// System.out.println("node: " + nodeId);
				
				idArray[Integer.parseInt(vId.get(i))] = (int) nodeId;

			}

			time = System.currentTimeMillis() - time;

			System.out.println("Duration VRP: " + time + " ms");

			System.out.println("finished VRP");
			
			// ######### Route insert #########
			
			Map<String, Object> propertiesRoute = new HashMap<>();
			
			System.out.println("Inserting Route data");
			
			time = System.currentTimeMillis();
			
			long nodeId;
			
			for(Ip1Node n1 : tree.list) {
				
				Map<String, Object> ip1Map = new HashMap<>();
				
				ip1Map.put("ip", n1.value);
				
				nodeId = inserter.createNode(ip1Map, ip1Label);
				
				n1.setId(nodeId);
				
				for(Ip2Node n2 : n1.next) {
					
					Map<String, Object> ip2Map = new HashMap<>();
					
					ip2Map.put("ip", n2.value);
					
					nodeId = inserter.createNode(ip2Map, ip2Label);
					
					n2.setId(nodeId);
					
					for(Ip3Node n3 : n2.next) {
						
						Map<String, Object> ip3Map = new HashMap<>();
						
						ip3Map.put("ip", n3.value);
						
						nodeId = inserter.createNode(ip3Map, ip3Label);
						
						n3.setId(nodeId);
						
						for(Ip4Node n4 : n3.next) {
							
							Map<String, Object> ip4Map = new HashMap<>();
							
							ip4Map.put("ip", n4.value);
							
							nodeId = inserter.createNode(ip4Map, ip4Label);
							
							n4.setId(nodeId);
														
							for(LengthNode ln : n4.next) {
								
								propertiesRoute.put("asn",ln.next.asn);
								propertiesRoute.put("validity",ln.next.val);
								propertiesRoute.put("rir",ln.next.rir);
								propertiesRoute.put("ipver",ln.next.ipver);
								propertiesRoute.put("binary",ln.next.bin);
																
								nodeId = inserter.createNode(propertiesRoute, routeLabel);
								
								ln.next.setId(nodeId);

								for (Integer vrp : ln.next.next) {
									inserter.createRelationship(nodeId, (long)idArray[vrp], hasVrp, null);
								}
								
								Map<String, Object> lnMap = new HashMap<>();
								
								lnMap.put("len", ln.value);
								
								nodeId = inserter.createNode(lnMap, lengthLabel);
								
								ln.setId(nodeId);
								
								inserter.createRelationship(ln.id, ln.next.id, routeData, null);
								
								inserter.createRelationship(n4.id, ln.id, lengthRel, null);
								
							}
							
							inserter.createRelationship(n3.id, n4.id, ip4Rel, null);
							
						}
						
						inserter.createRelationship(n2.id, n3.id, ip3Rel, null);
						
					}	
					
					inserter.createRelationship(n1.id, n2.id, ip2Rel, null);
					
				}
				
				inserter.createRelationship(rootId, n1.id, ip1Rel, null);
				
			}
			
			time = System.currentTimeMillis() - time;
			
			System.out.println("Duration routes: " + time + " ms");
			
			System.out.println("finished routes");

		}
		catch(Exception e) {
			System.out.println(e);
		}		
		finally {
			// ######### Inserter shutdown #########
			
			time = System.currentTimeMillis();
			
			System.out.println("shutting down inserter");
			
			inserter.shutdown();
			
			time = System.currentTimeMillis() - time;
			
			System.out.println("inserter shutdown");
			
			System.out.println("Duration inserter shutdown: " + time + "ms");			
		}
		
		allTime = System.currentTimeMillis() - allTime;
		
		System.out.println("Duration for all operations: " + allTime + "ms");
		
	}
	
	private static int countNodes(Tree tree) {
		int sum = 1;
		
//		System.out.println("Root - " + tree.list.size());
		sum += tree.list.size();
				
		for(Ip1Node n1 : tree.list) {
			
//			System.out.println("n1 " + n1.value + " - " + n1.next.size());
			sum += n1.next.size();
			
			for(Ip2Node n2 : n1.next) {
				
//				System.out.println("n2 " + n2.value + " - " + n2.next.size());
				sum += n2.next.size();
				
				for(Ip3Node n3 : n2.next) {
					
//					System.out.println("n3 " + n3.value + " - " + n3.next.size());
					sum += n3.next.size();
					
					for(Ip4Node n4 : n3.next) {
						
//						System.out.println("n4 " + n4.value + " - " + n4.next.size());
						sum += n4.next.size();
						sum ++;
						
						for(LengthNode ln : n4.next) {
							
							for(Integer vrp : ln.next.next) {
								sum++;
							}
						}
					}
				}
			}
			
		}
		return sum;
	}
	
	private static void printTree(Tree tree) {
		for(Ip1Node n1 : tree.list) {
			System.out.println(n1.value);
			for(Ip2Node n2 : n1.next) {
				System.out.println("\t" + n2.value);
				for(Ip3Node n3 : n2.next) {
					System.out.println("\t\t" + n3.value);
					for(Ip4Node n4 : n3.next) {
						System.out.println("\t\t\t" + n4.value);
						for(LengthNode ln : n4.next) {
							System.out.println("\t\t\t\t" + ln.value);
							System.out.println("\t\t\t\t\t" + ln.next.asn);
							System.out.println("\t\t\t\t\t" + ln.next.val);
							System.out.println("\t\t\t\t\t" + ln.next.rir);
							System.out.println("\t\t\t\t\t" + ln.next.ipver);
							System.out.println("\t\t\t\t\t" + ln.next.bin);
							System.out.print("\t\t\t\t\t");
							for(Integer vrp : ln.next.next) {
								System.out.print(vrp + " ");
							}
							System.out.print("\n");
						}
					}
				}
			}
			
		}
	}
	
	private static void listToTree(Tree tree) {
		
		for(int i=0; i<rAsn.size(); i++) {
			String[] parts = rPrefix.get(i).split("/");
			String[] ip = parts[0].split("\\.");
//			System.out.println(ip[0] + " " + ip[1] + " " + ip[2] + " " + ip[3] + " - " + parts[1]);
			
			List<Integer> vrp = new ArrayList<Integer>();
			
			String toSplit = rVrp.get(i).replaceAll("\\s", "");
			
			String[] splittedVrp = toSplit.split(",");
			
			if(!splittedVrp[0].equals("")) {
				for (int j=0; j<splittedVrp.length; j++)
					vrp.add(Integer.parseInt(splittedVrp[j]));
			}
			
			RouteNode routeNode = new RouteNode(vrp, Integer.parseInt(rAsn.get(i)), rVal.get(i), rRir.get(i), Integer.parseInt(rIpver.get(i)), rBinary.get(i), 0);
			LengthNode ln = new LengthNode(routeNode, Integer.parseInt(parts[1]), 0);
			
		
			Ip1Node ip1;
			Ip2Node ip2;
			Ip3Node ip3;
			Ip4Node ip4;
			
			if(!tree.hasNode(Integer.parseInt(ip[0]))) {
//				System.out.println("Ip1 new " + Integer.parseInt(ip[0]));
				ip1 = new Ip1Node(Integer.parseInt(ip[0]), 0);
			}
			else {
//				System.out.println("Ip1 old " + Integer.parseInt(ip[0]));
				ip1 = tree.getNode(Integer.parseInt(ip[0]));
			}
			
			if(!ip1.hasNode(Integer.parseInt(ip[1]))) {
//				System.out.println("Ip2 new " + Integer.parseInt(ip[1]));
				ip2 = new Ip2Node(Integer.parseInt(ip[1]), 0);
			}
			else {
//				System.out.println("Ip2 old " + Integer.parseInt(ip[1]));
				ip2 = ip1.getNode(Integer.parseInt(ip[1]));
			}
			
			if(!ip2.hasNode(Integer.parseInt(ip[2]))) {
//				System.out.println("Ip3 new " + Integer.parseInt(ip[2]));
				ip3 = new Ip3Node(Integer.parseInt(ip[2]), 0);
			}
			else {
//				System.out.println("Ip3 old " + Integer.parseInt(ip[2]));
				ip3 = ip2.getNode(Integer.parseInt(ip[2]));
			}
			
			if(!ip3.hasNode(Integer.parseInt(ip[3]))) {
//				System.out.println("Ip4 new " + Integer.parseInt(ip[3]));
				ip4 = new Ip4Node(Integer.parseInt(ip[3]), 0);
			}
			else {
//				System.out.println("Ip4 old " + Integer.parseInt(ip[3]));
				ip4 = ip3.getNode(Integer.parseInt(ip[3]));
			}
			
			ip4.insertNode(ln);
			ip3.insertNode(ip4);
			ip2.insertNode(ip3);
			ip1.insertNode(ip2);
			tree.insertNode(ip1);
			
			/*
			ip4 = new Ip4Node(Integer.parseInt(ip[3]), 0);
			ip4.insertNode(ln);
			
			ip3 = new Ip3Node(Integer.parseInt(ip[2]), 0);
			ip3.insertNode(ip4);
			
			ip2 = new Ip2Node(Integer.parseInt(ip[1]), 0);
			ip2.insertNode(ip3);
			
			ip1 = new Ip1Node(Integer.parseInt(ip[0]), 0);
			ip1.insertNode(ip2);
			
			tree.insertNode(ip1);
			*/
		}
			
		
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

			while (line != null) {

				if (line.contains("ROUTES")) {
					route = true;
				} else if (line.contains("VRP")) {
					route = false;
				} else {
					String[] content = line.split(";");

					if (route) {
						rAsn.add(content[0]);
						rPrefix.add(content[1]);
						rVal.add(content[2]);
						rRir.add(content[3]);
						rVrp.add(content[4]);
						rIpver.add(content[5]);
						rBinary.add(content[6]);
					} else {
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
	
	private static class RouteNode {
		
		List<Integer> next;
		int asn;
		String val;
		String rir;
		int ipver;
		String bin;
		long id;
		
		private RouteNode(List<Integer> next, int asn, String val, String rir, int ipver, String bin, long id) {
			this.next = next;
			this.asn = asn;
			this.val = val;
			this.rir = rir;
			this.ipver = ipver;
			this.bin = bin;
			this.id = id;
		}
		
		private void setId(long id) {
			this.id = id;
		}
		
	}
	
	private static class LengthNode {
		
		RouteNode next;
		int value;
		long id;
		
		private void setId(long id) {
			this.id = id;
		}
		
		private LengthNode(RouteNode next, int value, long id) {
			this.next = next;
			this.value = value;
			this.id = id;
		}
		
	}	
	
	private static class Ip4Node {
		
		List<LengthNode> next;
		int value;
		long id;
		
		private Ip4Node(int value, long id) {
			this.next = new ArrayList<LengthNode>();
			this.value = value;
			this.id = id;
		}
		
		private void setId(long id) {
			this.id = id;
		}
		
		private void insertNode(LengthNode node) {
			if(!hasNode(node.value)) {
				next.add(node);
			}
//			else {
//				LengthNode n = getNode(node.value);
//				n.next = node.next;
//			}
			
		}
		
		private boolean hasNode(int value) {
			LengthNode x = new LengthNode(null, value, 0);
			if(next.contains(x))
				return true;
			return false;
		}
		
		private LengthNode getNode(int value) {
			LengthNode x = new LengthNode(null, value, 0);
			for(LengthNode node : next) {
				if(node.equals(x))
					return node;
			}			
			return null;
		}
		
		@Override
		public boolean equals(Object object) {
			
			boolean sameSame = false;
			if (object != null && object instanceof LengthNode) {
				sameSame = this.value == ((LengthNode) object).value;
			}
			return sameSame;
		}
		
	}

	private static class Ip3Node {
		
		List<Ip4Node> next;
		int value;
		long id;
		
		private Ip3Node(int value, long id) {
			this.next = new ArrayList<Ip4Node>();
			this.value = value;
			this.id = id;
		}
		
		private void setId(long id) {
			this.id = id;
		}
		
		private void insertNode(Ip4Node node) {
			if(!hasNode(node.value)) {
				next.add(node);
			}
//			else {
//				Ip4Node n = getNode(node.value);
//				n.next.addAll(node.next);
//			}
		}
		
		private boolean hasNode(int value) {
			Ip4Node x = new Ip4Node(value, 0);
			if(next.contains(x))
				return true;
			return false;
		}
		
		private Ip4Node getNode(int value) {
			Ip4Node x = new Ip4Node(value, 0);
			for(Ip4Node node : next) {
				if(node.equals(x))
					return node;
			}			
			return null;
		}
		
		@Override
		public boolean equals(Object object) {
			
			boolean sameSame = false;
			if (object != null && object instanceof Ip3Node) {
				sameSame = this.value == ((Ip3Node) object).value;
			}
			return sameSame;
		}
		
	}
	
	private static class Ip2Node {
		
		List<Ip3Node> next;
		int value;
		long id;
		
		private Ip2Node(int value, long id) {
			this.next = new ArrayList<Ip3Node>();
			this.value = value;
			this.id = id;
		}
		
		private void setId(long id) {
			this.id = id;
		}
		
		private void insertNode(Ip3Node node) {
			if(!hasNode(node.value)) {
				next.add(node);
			}
//			else {
//				Ip3Node n = getNode(node.value);
//				n.next.addAll(node.next);
//			}
		}
		
		private boolean hasNode(int value) {
			Ip3Node x = new Ip3Node(value, 0);
			if(next.contains(x))
				return true;
			return false;
		}
		
		private Ip3Node getNode(int value) {
			Ip3Node x = new Ip3Node(value, 0);
			for(Ip3Node node : next) {
				if(node.equals(x))
					return node;
			}			
			return null;
		}
		
		@Override
		public boolean equals(Object object) {
			
			boolean sameSame = false;
			if (object != null && object instanceof Ip2Node) {
				sameSame = this.value == ((Ip2Node) object).value;
			}
			return sameSame;
		}
		
	}
	
	private static class Ip1Node {
		
		List<Ip2Node> next;
		int value;
		long id;
		
		private Ip1Node(int value, long id) {
			this.next = new ArrayList<Ip2Node>();
			this.value = value;
			this.id = id;
		}
		
		private void setId(long id) {
			this.id = id;
		}
		
		private void insertNode(Ip2Node node) {
			if(!hasNode(node.value)) {
				next.add(node);
			}
//			else {
//				Ip2Node n = getNode(node.value);
//				n.next.addAll(node.next);
//			}
		}
		
		private boolean hasNode(int value) {
			Ip2Node x = new Ip2Node(value, 0);
			if(next.contains(x))
				return true;
			return false;
		}
		
		private Ip2Node getNode(int value) {
			Ip2Node x = new Ip2Node(value, 0);
			for(Ip2Node node : next) {
				if(node.equals(x))
					return node;
			}			
			return null;
		}

		@Override
		public boolean equals(Object object) {
			
			boolean sameSame = false;
			if (object != null && object instanceof Ip1Node) {
				sameSame = this.value == ((Ip1Node) object).value;
			}
			return sameSame;
		}
	}
	
	private static class Tree {
		
		List<Ip1Node> list;
		
		private Tree() {
			this.list = new ArrayList<Ip1Node>();
		}
		
		private void insertNode(Ip1Node node) {
			if(!hasNode(node.value)) {
				list.add(node);
			}
//			else {
//				Ip1Node n = getNode(node.value);
//				n.next.addAll(node.next);
//			}
		}
		
		private boolean hasNode(int value) {
			Ip1Node x = new Ip1Node(value, 0);
			if(list.contains(x))
				return true;
			return false;
		}
		
		private Ip1Node getNode(int value) {
			Ip1Node x = new Ip1Node(value, 0);
			for(Ip1Node node : list) {
				if(node.equals(x))
					return node;
			}			
			return null;
		}
		
	}
	
	private static void checkVrpOccurance() {

		for (int i = 0; i < rVrp.size(); i++) {
			String toSplit = rVrp.get(i).replaceAll("\\s", "");
			String[] splitted = toSplit.split(",");

			if (!splitted[0].equals("")) {
				System.out.print("index: " + (i + 2) + "\n\t");
				for (String item : splitted) {
					System.out.print(item + ", ");
				}
				System.out.println();
			}
		}

	}

}

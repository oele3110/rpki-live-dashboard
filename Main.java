import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class Main {

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

	public static void main(String[] args) {
		
		String file = "queries";
		
		System.out.println("##### Read File #####");
		
		long time = System.currentTimeMillis();
		
		readFile(file);
		
		time = System.currentTimeMillis() - time;
		
		System.out.println("Duration: " + time + " ms");
	
		System.out.println("\n\n##### SQL #####");
		
		time = System.currentTimeMillis();
		
		db();
		
		time = System.currentTimeMillis() - time;
		
		System.out.println("Duration: " + time + " ms");
		
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
	
	private static void db() {
		
		Connection con = null;
		long time;
		
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			
			con = DriverManager.getConnection("jdbc:mysql://localhost/bgp", "root","skims12345");
			
			System.out.println("Start inserting values");
			
			Statement s = con.createStatement();
			PreparedStatement ps = null;
			
			// commit manually
			con.setAutoCommit(false);
			
			// ################################ Route ################################
			
			System.out.println("Truncate Route data");
			
			s.executeUpdate("TRUNCATE test_routes");
			
			
			System.out.println("Inserting Route data");
			
			ps = con.prepareStatement("INSERT INTO test_routes VALUES (?, ?, ?, ?, ?, ?, ?)");
			
			time = System.currentTimeMillis();
			
			for(int i=0; i<rAsn.size(); i++) {
				ps.setString(1, rAsn.get(i));
				ps.setString(2, rPrefix.get(i));
				ps.setString(3, rVal.get(i));
				ps.setString(4, rRir.get(i));
				ps.setString(5, rVrp.get(i));
				ps.setString(6, rIpver.get(i));
				ps.setString(7, rBinary.get(i));
				ps.addBatch();
			}
						
			ps.executeBatch();
			con.commit();
			
			time = System.currentTimeMillis() - time;
			
			System.out.println("Duration Routes: " + time + " ms");
			
			System.out.println("finished routes");
			
			// ################################ VRP ################################ 
			
			
			System.out.println("Truncate VRP data");
			
			s.executeUpdate("TRUNCATE test_vrp");
			
			
			System.out.println("Inserting VRP data");
			
			ps = con.prepareStatement("INSERT INTO test_vrp VALUES (?, ?, ?, ?, ?, ?)");
						
			time = System.currentTimeMillis();
			
			for(int i=0; i<vId.size(); i++) {
				ps.setString(1, vId.get(i));
				ps.setString(2, vAsn.get(i));
				ps.setString(3, vPrefix.get(i));
				ps.setString(4, vMax.get(i));
				ps.setString(5, vMin.get(i));
				ps.setString(6, vBinary.get(i));
				ps.addBatch();
			}
			
			ps.executeBatch();
			con.commit();
			
			time = System.currentTimeMillis() - time;
			
			System.out.println("Duration VRP: " + time + " ms");
			
			System.out.println("finished VRP");
			
			System.out.println("finished");
			
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}


package OSMParser;

/*
 * This is my OSMParser which is used to import OSM file into HBase.
 * */

import java.util.ArrayList;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;
import javax.xml.parsers.ParserConfigurationException;

import models.Element;

public class OSMParser extends DefaultHandler {

	// Create tables and set names

	private Map<String, Element> elements;
	private Element currentElement = new Element();
	private String temp;
	static TableName nodesTableName = TableName.valueOf("nodes_HBase");
	static TableName waysTableName = TableName.valueOf("ways_HBase");
	static Connection connection;
	Table nodesTable;
	Table waysTable;

	public Map<String, Element> parse(File f) throws IOException, SAXException,
			ParserConfigurationException {

		if (!f.exists() || !f.isFile()) {
			throw new FileNotFoundException();
		}
		if (!f.canRead()) {
			throw new IOException("File is unreadable");
		}
		return parse(new InputSource(new FileReader(f)));

	}

	public Map<String, Element> parse(InputSource input) throws IOException,
			SAXException, ParserConfigurationException {
		elements = new HashMap<String, Element>();

		XMLReader xmlReader = XMLReaderFactory.createXMLReader();
		xmlReader.setContentHandler(this);
		xmlReader.setErrorHandler(this);
		xmlReader.parse(input);

		return elements;
	}

	public void startElement(String uri, String localName, String qName,
			Attributes attributes) {

		
		//Set up attributes for node_HBase tables
		if (qName.equals("node")) {
			currentElement.setIsWay(false);
			currentElement.setId(attributes.getValue("id")); 
			currentElement.setUser(attributes.getValue("user"));
			currentElement.setUid(attributes.getValue("uid"));
			currentElement.setLat(attributes.getValue("lat"));
			currentElement.setLon(attributes.getValue("lon"));
		} else if (qName.equals("way")) {
			currentElement.setIsWay(true);
			currentElement.setId(attributes.getValue("id")); // Set up attributes for
														// way_Hbase tables
			currentElement.setUser(attributes.getValue("user"));
			currentElement.setUid(attributes.getValue("uid"));
		} else if (qName.equals("nd")) {
			currentElement.getWayNodes().add(attributes.getValue("ref"));
		} else if (qName.equals("tag")) {
			currentElement.getTags().put(attributes.getValue("k"),
					attributes.getValue("v"));
		}
	}

	public void characters(char[] buffer, int start, int length) {

		temp = new String(buffer, start, length);
	}

	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if (qName.equals("node")) {
			Put row = new Put(Bytes.toBytes(currentElement.getId()));
			row.addColumn(Bytes.toBytes("nodeData"), Bytes.toBytes("user"),
					Bytes.toBytes(currentElement.getUser()));
			row.addColumn(Bytes.toBytes("nodeData"), Bytes.toBytes("uid"),
					Bytes.toBytes(currentElement.getUid()));
			row.addColumn(Bytes.toBytes("nodeData"), Bytes.toBytes("lat"),
					Bytes.toBytes(currentElement.getLat()));
			row.addColumn(Bytes.toBytes("nodeData"), Bytes.toBytes("lon"),
					Bytes.toBytes(currentElement.getLon()));
			if (!currentElement.getIsWay() && currentElement.getTags().size() > 0) {
				HashMap<String, String> tags = currentElement.getTags();

				String tagsString = "";
				for (String key : tags.keySet()) {
					String value = tags.get(key);
					tagsString += "[" + key + "=" + value + "],";
				}
				row.addColumn(Bytes.toBytes("nodeData"), Bytes.toBytes("tag"),
						Bytes.toBytes(tagsString));
			}
			try {
				nodesTable.put(row);
				currentElement = new Element();
			} catch (IOException ex) {
				ex.printStackTrace();
			}

		} else if (qName.equals("way")) {

			// create column nd
			Put row = new Put(Bytes.toBytes(currentElement.getId()));
			row.addColumn(Bytes.toBytes("wayData"), Bytes.toBytes("user"),
					Bytes.toBytes(currentElement.getUser()));
			row.addColumn(Bytes.toBytes("wayData"), Bytes.toBytes("uid"),
					Bytes.toBytes(currentElement.getUid()));
			if (currentElement.getWayNodes().size() > 0) {

				ArrayList<String> nodes = (ArrayList<String>) currentElement
						.getWayNodes();
				String nodeString = "";
				for (String node : nodes) {
					nodeString += node + ",";
				}

				row.addColumn(Bytes.toBytes("wayData"), Bytes.toBytes("nd"),
						Bytes.toBytes(nodeString));
			}
			if (currentElement.getIsWay() && currentElement.getTags().size() > 0) {
				HashMap<String, String> tags = currentElement.getTags(); // Use Hashmap

				// create column 'tag' for wayData
				String tagsString = "";
				for (String key : tags.keySet()) {
					String value = tags.get(key);
					tagsString += "[" + key + "=" + value + "],";
				}
				row.addColumn(Bytes.toBytes("wayData"), Bytes.toBytes("tag"),
						Bytes.toBytes(tagsString));
			}
			try {
				waysTable.put(row);
				currentElement = new Element();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	// Set up HBase
	public void setUpHbase() throws IOException {

		Configuration config = HBaseConfiguration.create();
		config.addResource("core-site.xml"); // add configuration files to HBase
		config.addResource("hbase-site.xml");
		config.addResource("hdfs-site.xml");
		connection = ConnectionFactory.createConnection(config);
		Admin hBaseAdmin = connection.getAdmin(); // connect to HBase

		
		// Delete table if it
		// already exists,
		// disable table first
		// and then delete
		if (hBaseAdmin.tableExists(nodesTableName)) { 
			hBaseAdmin.disableTable(nodesTableName);
			hBaseAdmin.deleteTable(nodesTableName);
		}
		HTableDescriptor nodesDescriptor = new HTableDescriptor(nodesTableName); 
		
//		Add Column family
		nodesDescriptor.addFamily(new HColumnDescriptor("nodeData"));
		hBaseAdmin.createTable(nodesDescriptor);

		// if table already exists, disable first and then delete the table
		if (hBaseAdmin.tableExists(waysTableName)) {
			hBaseAdmin.disableTable(waysTableName);
			hBaseAdmin.deleteTable(waysTableName);

		}

		// Create nodesTable and waysTable
		HTableDescriptor waysDescriptor = new HTableDescriptor(waysTableName);
		waysDescriptor.addFamily(new HColumnDescriptor("wayData"));
		hBaseAdmin.createTable(waysDescriptor);

		nodesTable = connection.getTable(nodesTableName);
		waysTable = connection.getTable(waysTableName);
	}

	public static void main(String[] args) throws Exception {

		OSMParser parser = new OSMParser();
		parser.setUpHbase();
		parser.parse(new File(System
				.getProperty("home/cloudera/workspace/map.osm"))); 
		// import osm file into HBase
																	
		System.out.println("succeed");
	}
}

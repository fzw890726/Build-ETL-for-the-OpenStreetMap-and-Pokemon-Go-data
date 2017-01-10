package OSMParser;

/*
 * This is the pokemon Parser to import kml file into HBase 
 * 
 * */

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import com.google.common.io.Files;
import models.PlacemarkElement;
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

public class PokemonParser extends DefaultHandler {

	// Create table for kml data
	private int id = 0;
	private Map<String, PlacemarkElement> elements;
	private PlacemarkElement currentElement;
	private String temp;
	static TableName pokemonTableName = TableName.valueOf("placeMark_HBase");
	static Connection connection;
	Table pokemonTable;

//	verification
	public Map<String, PlacemarkElement> parse(File f) throws IOException,
			SAXException, ParserConfigurationException {
		if (!f.exists() || !f.isFile()) {
			throw new FileNotFoundException();
		}
		if (!f.canRead()) {
			throw new IOException("file is unreadable");
		}
		return parse(new InputSource(new FileReader(f)));

	}

	public Map<String, PlacemarkElement> parse(InputSource input)
			throws IOException, SAXException, ParserConfigurationException {
		elements = new HashMap<String, PlacemarkElement>();

		// Use XML reader
		XMLReader xmlReader = XMLReaderFactory.createXMLReader();
		xmlReader.setContentHandler(this);
		xmlReader.setErrorHandler(this);
		xmlReader.parse(input);

		return elements;
	}

	
//	Create PlacemarkElement
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) {
		if (qName.equals("Placemark")) {
			currentElement = new PlacemarkElement();
		}
	}

	public void characters(char[] buffer, int start, int length) {
		temp = new String(buffer, start, length);
	}

	// Create Column Families Placemark
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if (qName.equals("name")) {
			if (currentElement != null) {
				currentElement.setName(temp);
			}
		} else if (qName.equals("description")) {
			if (currentElement != null) {
				currentElement.setDescription(temp);
			}
		} else if (qName.equals("coordinates")) {
			if (temp.contains(",")) {
				String coordinates = temp;
				String[] coordinatesArray = coordinates.split(",");
				currentElement.setLon(coordinatesArray[0]);
				currentElement.setLat(coordinatesArray[1]);

			}

		} else if (qName.equals("Placemark")) {
			Put row = new Put(Bytes.toBytes(id));
			row.addColumn(Bytes.toBytes("placeData"), Bytes.toBytes("name"),
					Bytes.toBytes(currentElement.getName()));
			if (currentElement.getDescription() != null) {
				row.addColumn(Bytes.toBytes("placeData"),
						Bytes.toBytes("description"),
						Bytes.toBytes(currentElement.getDescription()));

			}
			if (currentElement.getLon() != null) {
				row.addColumn(Bytes.toBytes("placeData"), Bytes.toBytes("lon"),
						Bytes.toBytes(currentElement.getLon()));
			}
			if (currentElement.getLat() != null) {
				row.addColumn(Bytes.toBytes("placeData"), Bytes.toBytes("lat"),
						Bytes.toBytes(currentElement.getLat()));
			}

			try {
				pokemonTable.put(row);
				currentElement = null;
				id++;
			} catch (IOException ex) {
				ex.printStackTrace();
			}

		}
	}

	// Set up HBase
	public void setUpHbase() throws IOException {

		// Add configuration resource HBase
		Configuration config = HBaseConfiguration.create();
		config.addResource("core-site.xml");
		config.addResource("hbase-site.xml");
		config.addResource("hdfs-site.xml");
		connection = ConnectionFactory.createConnection(config);
		Admin hBaseAdmin = connection.getAdmin();

		// If table has already existed, disable table first and then delete
		// it.
		if (hBaseAdmin.tableExists(pokemonTableName)) {
			hBaseAdmin.disableTable(pokemonTableName);
			hBaseAdmin.deleteTable(pokemonTableName);

		}
		HTableDescriptor pokemonDescriptor = new HTableDescriptor(
				pokemonTableName);
		pokemonDescriptor.addFamily(new HColumnDescriptor("placeData"));
		hBaseAdmin.createTable(pokemonDescriptor);

		pokemonTable = connection.getTable(pokemonTableName);
	}

	public static void main(String[] args) throws Exception {

		// import kml data into HBase
		File file = new File(
				System.getProperty("home/cloudera/workspace/Parser/Houston Pokémon GO Map Pokéstops and Gyms.kml"));
		String string = Files.toString(file, Charset.defaultCharset());
		String newString = string.replace("\n</name>", "</name>");
		File newFile = new File(System.getProperty("user.dir") + "/pokemon.txt");
		Files.write(newString.getBytes(), newFile);

		PokemonParser parser = new PokemonParser();
		parser.setUpHbase();
		parser.parse(newFile);
		System.out.println("succeed");
	}
}
package OSMParser;
//With new changes
import com.google.common.io.Files;
import models.PlacemarkElement;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import java.io.*;
import java.nio.charset.Charset;
//import java.nio.file.Path;
//import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
//import java.util.stream.Collectors;

//import models.PlacemarkElement;
import org.xml.sax.helpers.XMLReaderFactory;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class PokemonParser extends DefaultHandler {

    private int id = 0;
    private Map<String, PlacemarkElement> elements;
    private PlacemarkElement current;
    private String temp;
    static TableName pokemonTableName = TableName.valueOf("placeMark");
    static Connection connection;
    Table pokemonTable;

    public Map<String, PlacemarkElement> parse(File f) throws IOException, SAXException
            , ParserConfigurationException {
        if(!f.exists() || !f.isFile()){
            throw new FileNotFoundException();
        }
        if(!f.canRead()){
            throw new IOException("Can't read file");
        }
        return parse(new InputSource(new FileReader(f)));

    }

    public Map<String, PlacemarkElement> parse(InputSource input) throws IOException,
            SAXException, ParserConfigurationException {
        elements = new HashMap<String, PlacemarkElement>();

        XMLReader xmlReader = XMLReaderFactory.createXMLReader();
        xmlReader.setContentHandler(this);
        xmlReader.setErrorHandler(this);
        xmlReader.parse(input);

        return elements;
    }

    @Override
    public void startElement(String uri, String localName,
                             String qName, Attributes attributes){
        if(qName.equals("Placemark")){
            current = new PlacemarkElement();
        }
    }

    @Override
    public void characters(char[] buffer, int start, int length) {
        temp = new String(buffer, start, length);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException{
        if (qName.equals("name")){
            if (current != null){
                current.setName(temp);
            }
        }else if (qName.equals("description")){
            if (current != null){
                current.setDescription(temp);
            }
        }else if (qName.equals("coordinates")){
            if(temp.contains(",")){
                String coordinates = temp;
                String[] coordinatesArray = coordinates.split(",");
                current.setLon(coordinatesArray[0]);
                current.setLat(coordinatesArray[1]);
                //System.out.println(coordinatesArray[0]+" "+coordinatesArray[1]);
            }

        }else if(qName.equals("Placemark")){
            Put row = new Put(Bytes.toBytes(id));
            row.addColumn(Bytes.toBytes("placeData"), Bytes.toBytes("name"), Bytes.toBytes(current.getName()));
            if(current.getDescription() !=null){
                row.addColumn(Bytes.toBytes("placeData"), Bytes.toBytes("description"), Bytes.toBytes(current.getDescription()));

            }
            if(current.getLon() !=null){
                row.addColumn(Bytes.toBytes("placeData"), Bytes.toBytes("lon"), Bytes.toBytes(current.getLon()));            }
            if(current.getLat() !=null){
                row.addColumn(Bytes.toBytes("placeData"), Bytes.toBytes("lat"), Bytes.toBytes(current.getLat()));
            }

           try{
                pokemonTable.put(row);
                current = null;
                id++;
            }catch (IOException ex){
                ex.printStackTrace();
            }

        }
    }

    public void setUpHbase() throws IOException{

        Configuration config = HBaseConfiguration.create();
        config.addResource("core-site.xml");
        config.addResource("hbase-site.xml");
        config.addResource("hdfs-site.xml");
        connection = ConnectionFactory.createConnection(config);
        Admin hBaseAdmin = connection.getAdmin();

        if(hBaseAdmin.tableExists(pokemonTableName)){
            hBaseAdmin.disableTable(pokemonTableName);
            hBaseAdmin.deleteTable(pokemonTableName);

        }
        HTableDescriptor pokemonDescriptor = new HTableDescriptor(pokemonTableName);
        pokemonDescriptor.addFamily(new HColumnDescriptor("placeData"));
        hBaseAdmin.createTable(pokemonDescriptor);

        pokemonTable = connection.getTable(pokemonTableName);
    }

    public static void main(String[] args) throws Exception{

        File file = new File(System.getProperty("user.dir")+"/Houston Pokémon GO Map Pokéstops and Gyms.kml");
        String string = Files.toString(file, Charset.defaultCharset());
        String newString = string.replace("\n</name>", "</name>");
        File newFile = new File(System.getProperty("user.dir")+"/pokemon.txt");
        Files.write(newString.getBytes(), newFile);

        PokemonParser parser = new PokemonParser();
        parser.setUpHbase();
        parser.parse(newFile);
        System.out.println("complete");
    }
}

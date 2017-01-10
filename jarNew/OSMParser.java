package OSMParser;
//With new changes
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
import java.util.ArrayList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
//import java.util.stream.Collectors;

import models.Element;
import org.xml.sax.helpers.XMLReaderFactory;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class OSMParser extends DefaultHandler{

    private Map<String, Element> elements;
    private Element current = new Element();
    private String temp;
    static TableName nodesTableName = TableName.valueOf("nodes");
    static TableName waysTableName = TableName.valueOf("ways");
    static Connection connection;
    Table nodesTable;
    Table waysTable;

    public Map<String, Element> parse(File f) throws IOException, SAXException
            , ParserConfigurationException{
        if(!f.exists() || !f.isFile()){
            throw new FileNotFoundException();
        }
        if(!f.canRead()){
            throw new IOException("Can't read file");
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

    @Override
    public void startElement(String uri, String localName,
                             String qName, Attributes attributes){
        if(qName.equals("node")){
            current.setIsWay(false);
            current.setId(attributes.getValue("id"));
            current.setUser(attributes.getValue("user"));
            current.setUid(attributes.getValue("uid"));
            current.setLat(attributes.getValue("lat"));
            current.setLon(attributes.getValue("lon"));
        }else if (qName.equals("way")){
            current.setIsWay(true);
            current.setId(attributes.getValue("id"));
            current.setUser(attributes.getValue("user"));
            current.setUid(attributes.getValue("uid"));
        }else if (qName.equals("nd")){
            current.getWayNodes().add(attributes.getValue("ref"));
        }else if (qName.equals("tag")){
            current.getTags().put(attributes.getValue("k"), attributes.getValue("v"));
        }
    }

    @Override
    public void characters(char[] buffer, int start, int length) {
        temp = new String(buffer, start, length);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException{
        if(qName.equals("node")){
            Put row = new Put(Bytes.toBytes(current.getId()));
            row.addColumn(Bytes.toBytes("nodeData"), Bytes.toBytes("user"), Bytes.toBytes(current.getUser()));
            row.addColumn(Bytes.toBytes("nodeData"), Bytes.toBytes("uid"), Bytes.toBytes(current.getUid()));
            row.addColumn(Bytes.toBytes("nodeData"), Bytes.toBytes("lat"), Bytes.toBytes(current.getLat()));
            row.addColumn(Bytes.toBytes("nodeData"), Bytes.toBytes("lon"), Bytes.toBytes(current.getLon()));
            if(!current.getIsWay() && current.getTags().size() > 0){
                HashMap<String, String> tags = current.getTags();
                //String tagsString = tags.keySet().stream().map(e -> e.toString()).collect(Collectors.joining(","));
                String tagsString ="";
                for(String key : tags.keySet()){
                	String value = tags.get(key);
                	tagsString +="["+key+"="+value+"],";
                }
                row.addColumn(Bytes.toBytes("nodeData"), Bytes.toBytes("tag"), Bytes.toBytes(tagsString));
            }
            try{
                nodesTable.put(row);
                current = new Element();
            }catch (IOException ex){
                ex.printStackTrace();
            }

        }else if(qName.equals("way")){

            Put row = new Put(Bytes.toBytes(current.getId()));
            row.addColumn(Bytes.toBytes("wayData"), Bytes.toBytes("user"), Bytes.toBytes(current.getUser()));
            row.addColumn(Bytes.toBytes("wayData"), Bytes.toBytes("uid"), Bytes.toBytes(current.getUid()));
            if(current.getWayNodes().size() > 0){
                //String wayNodes = String.join( ",", current.getWayNodes());
                ArrayList<String> nodes = (ArrayList<String>) current.getWayNodes();
                String nodeString ="";
                 for(String node:nodes){
                 	nodeString += node+",";
                 }
                //row.addColumn(Bytes.toBytes("wayData"), Bytes.toBytes("nd"), Bytes.toBytes(wayNodes));
                 row.addColumn(Bytes.toBytes("wayData"), Bytes.toBytes("nd"), Bytes.toBytes(nodeString));
            }
            if(current.getIsWay()&&current.getTags().size() > 0){
                HashMap<String, String> tags = current.getTags();
                //String tagsString = tags.keySet().stream().map(e -> e.toString()).collect(Collectors.joining(","));
                String tagsString ="";
                for(String key : tags.keySet()){
                	String value = tags.get(key);
                	tagsString +="["+key+"="+value+"],";
                }
                row.addColumn(Bytes.toBytes("wayData"), Bytes.toBytes("tag"), Bytes.toBytes(tagsString));
            }
            try{
                waysTable.put(row);
                current = new Element();
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

        if(hBaseAdmin.tableExists(nodesTableName)){
            hBaseAdmin.disableTable(nodesTableName);
            hBaseAdmin.deleteTable(nodesTableName);
        }
        HTableDescriptor nodesDescriptor = new HTableDescriptor(nodesTableName);
        nodesDescriptor.addFamily(new HColumnDescriptor("nodeData"));
        hBaseAdmin.createTable(nodesDescriptor);

        if(hBaseAdmin.tableExists(waysTableName)){
            hBaseAdmin.disableTable(waysTableName);
            hBaseAdmin.deleteTable(waysTableName);

        }
        HTableDescriptor waysDescriptor = new HTableDescriptor(waysTableName);
        waysDescriptor.addFamily(new HColumnDescriptor("wayData"));
        hBaseAdmin.createTable(waysDescriptor);

        nodesTable = connection.getTable(nodesTableName);
        waysTable = connection.getTable(waysTableName);
    }

    public static void main(String[] args) throws Exception{

        OSMParser parser = new OSMParser();
        parser.setUpHbase();
        parser.parse(new File(System.getProperty("user.dir")+"/map"));
        System.out.println("complete");
    }
}


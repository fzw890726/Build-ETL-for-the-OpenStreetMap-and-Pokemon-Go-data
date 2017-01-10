package models;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Element {
	private boolean isWay;
	private String id;
	private String user;
	private String uid;
	private String lat;
	private String lon;
	private HashMap<String, String> Tags;
	private List<String> wayNodes;

	public Element() {
		Tags = new HashMap<String, String>();
		wayNodes = new ArrayList<String>();
	}

	public boolean getIsWay() {
		return isWay;
	}

	public String getId() {
		return id;
	}

	public String getUser() {
		return user;
	}

	public String getUid() {
		return uid;
	}

	public String getLat() {
		return lat;
	}

	public String getLon() {
		return lon;
	}

	public HashMap<String, String> getTags() {
		return Tags;
	}

	public List<String> getWayNodes() {
		return wayNodes;
	}

	public void setIsWay(boolean way) {
		isWay = way;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public void setLat(String lat) {
		this.lat = lat;
	}

	public void setLon(String lon) {
		this.lon = lon;
	}

	public void setTags(HashMap<String, String> tags) {
		Tags = tags;
	}

	public void setWayNodes(List<String> wayNodes) {
		this.wayNodes = wayNodes;
	}
}
package com.cassandraguide.hotel;

import static com.cassandraguide.hotel.Constants.CAMBRIA_NAME;
import static com.cassandraguide.hotel.Constants.CL;
import static com.cassandraguide.hotel.Constants.CLARION_NAME;
import static com.cassandraguide.hotel.Constants.UTF8;
import static com.cassandraguide.hotel.Constants.WALDORF_NAME;
import static com.cassandraguide.hotel.Constants.W_NAME;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.insert;
//import org.apache.cassandra.thrift.Clock;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

public class Prepolation {
	private static final Logger LOG = Logger.getLogger(Prepolation.class);
	
	private Cassandra.Client client;
	private Connector connector;
	
	public Prepolation() throws Exception{
		connector = new Connector();
		client = connector.connect();
	}
	
	public void prepolate() throws Exception{
//		InsertAllHotels();
		InsertCityIndexes();
//		InsertAllPointOfInterest();
		connector.close();
	}

	public void InsertAllPointOfInterest() {
		// TODO Auto-generated method stub
		
	}

	public void InsertCityIndexes() throws Exception {
		// TODO Auto-generated method stub
		String scottsdaleKey = "Scottsdale:A2";
		String sfKey = "San Fransico:CA";
		String newYorkkey = "New York:NY";
		
		InsertCityIndexes(scottsdaleKey,CAMBRIA_NAME);
		InsertCityIndexes(scottsdaleKey,CLARION_NAME);
		InsertCityIndexes(sfKey,W_NAME);
		InsertCityIndexes(newYorkkey, WALDORF_NAME);

	}

	private void InsertCityIndexes(String rowKey, String hotelName) throws UnsupportedEncodingException, InvalidRequestException, UnavailableException, TimedOutException, TException {
		// TODO Auto-generated method stub

		long time = System.nanoTime();
//		ByteBuffer _hotel = ByteBuffer.wrap(hotelName.getBytes(UTF8));
		Column nameCol = new Column();
		nameCol.setName(hotelName.getBytes(UTF8));
		nameCol.setValue(new byte[0]);
		nameCol.setTimestamp(time);
		ColumnOrSuperColumn nameCosc = new ColumnOrSuperColumn();
		nameCosc.column = nameCol;
		
		Mutation nameMut = new Mutation();
		nameMut.column_or_supercolumn = nameCosc;
		
		Map<String, Map<String, List<Mutation>>> mutationMap = 
				new HashMap<String, Map<String,List<Mutation>>>();
		
		Map<String, List<Mutation>> muts = new HashMap<String, List<Mutation>>();
		List<Mutation> cols = new ArrayList<Mutation>();
		
		cols.add(nameMut);
		
		String columnFamily = "HotelByCity";
		muts.put(columnFamily, cols);
		
		mutationMap.put(rowKey, muts);
		
		ColumnPath cp = new ColumnPath(columnFamily);
		
		cp.setColumn(hotelName.getBytes(UTF8));
		
		ColumnParent parent = new ColumnParent(columnFamily);
		
		Column col = new Column();
		col.setName(hotelName.getBytes(UTF8));
		col.setValue(new byte[0]);
		col.setTimestamp(time);
		client.insert(ByteBuffer.wrap(rowKey.getBytes()), parent, col, CL);
		LOG.debug("Insert HotelByCity index for " + hotelName);
		
	}

	private void InsertAllHotels() throws UnsupportedEncodingException, InvalidRequestException, UnavailableException, TimedOutException, TException {
		// TODO Auto-generated method stub
		LOG.debug("Insert POIs");
		
		insertPOIemireState();
		insertPOICentralPark();
		insertPOIPhoenixZoo();
		insertPOISpringTraining();
		
		LOG.debug("Done inserting POIs.");
	}

	private void insertPOISpringTraining() throws UnsupportedEncodingException, InvalidRequestException, UnavailableException, TimedOutException, TException {
		// TODO Auto-generated method stub
		Map<ByteBuffer, Map<String, List<Mutation>>> outerMap = 
				new HashMap<ByteBuffer, Map<String,List<Mutation>>>();
		
		List<Mutation> columnsAdd = new ArrayList<Mutation>();
		
		long time = System.nanoTime();
		String keyName = "Spring Training";
		
		Column descCol = new Column();
		descCol.setName("desc".getBytes(UTF8));
		descCol.setValue("Fun for baseball fans.".getBytes("UTF-8"));
		
		Column phone = new Column();
		phone.setName("phone".getBytes(UTF8));
		phone.setValue("623-333-3333".getBytes(UTF8));
		
		List<Column> cols = new ArrayList<Column>();
		cols.add(descCol);
		cols.add(phone);
		
		Map<String, List<Mutation>> innerMap = new HashMap<String, List<Mutation>>();
		
		Mutation columns = new Mutation();
		
		ColumnOrSuperColumn descCosc = new ColumnOrSuperColumn();
		
		SuperColumn sc = new SuperColumn();
		sc.setName(CAMBRIA_NAME.getBytes());
		sc.setColumns(cols);
		
		descCosc.super_column = sc;
		
		columns.setColumn_or_supercolumn(descCosc);
		columnsAdd.add(columns);
		
		String superCFName = "PointOfInterest";
		ColumnPath cp = new ColumnPath();
		
		cp.column_family = superCFName;
		cp.setSuper_column(CAMBRIA_NAME.getBytes());
		cp.setSuper_columnIsSet(true);
		
		innerMap.put(superCFName, columnsAdd);
		outerMap.put(ByteBuffer.wrap(keyName.getBytes()),innerMap);
		
		client.batch_mutate(outerMap, CL);
		LOG.debug("Done Insert Sprint Training");
		
	}

	private void insertPOIPhoenixZoo() throws InvalidRequestException, UnavailableException, TimedOutException, TException, UnsupportedEncodingException {
		// TODO Auto-generated method stub
		Map<ByteBuffer, Map<String, List<Mutation>>> outerMap =
				 new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
				 List<Mutation> columnsToAdd = new ArrayList<Mutation>();
				 long ts = System.currentTimeMillis();
				 String keyName = "Phoenix Zoo";
				 Column descCol = new Column();
				 descCol.setName("desc".getBytes(UTF8));
				 descCol.setValue("They have animals here.".getBytes("UTF-8"));
				 Column phoneCol = new Column();
				 phoneCol.setName("phone".getBytes(UTF8));
				 phoneCol.setValue("480-555-9999".getBytes(UTF8));

				 List<Column> cols = new ArrayList<Column>();
				 cols.add(descCol);
				 cols.add(phoneCol);

				 Map<String, List<Mutation>> innerMap =
				 new HashMap<String, List<Mutation>>();

				 String cambriaName = "Cambria Suites Hayden";

				 Mutation columns = new Mutation();
				 ColumnOrSuperColumn descCosc = new ColumnOrSuperColumn();
				 SuperColumn sc = new SuperColumn();
				 sc.setName(cambriaName.getBytes());
				 sc.columns = cols;

				 descCosc.super_column = sc;
				 columns.setColumn_or_supercolumn(descCosc);

				 columnsToAdd.add(columns);

				 String superCFName = "PointOfInterest";
				 ColumnPath cp = new ColumnPath();
				 cp.column_family = superCFName;
				 cp.setSuper_column(cambriaName.getBytes());
				 cp.setSuper_columnIsSet(true);

				 innerMap.put(superCFName, columnsToAdd);
				 outerMap.put(ByteBuffer.wrap(keyName.getBytes()), innerMap);

				 client.batch_mutate(outerMap, CL);

				 LOG.debug("Done inserting Phoenix Zoo.");
	}

	private void insertPOICentralPark() throws InvalidRequestException, UnavailableException, TimedOutException, TException, UnsupportedEncodingException {
		// TODO Auto-generated method stub
		Map<ByteBuffer, Map<String, List<Mutation>>> outerMap =
				 new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
				 List<Mutation> columnsToAdd = new ArrayList<Mutation>();

				 String keyName = "Central Park";
				 Column descCol = new Column();
				 descCol.setName("desc".getBytes(UTF8));
				 descCol.setValue("Walk around in the park. It's pretty.".getBytes("UTF-8"));
				 List<Column> cols = new ArrayList<Column>();
				 cols.add(descCol);

				 Map<String, List<Mutation>> innerMap =
				 new HashMap<String, List<Mutation>>();

				 Mutation columns = new Mutation();
				 ColumnOrSuperColumn descCosc = new ColumnOrSuperColumn();
				 SuperColumn waldorfSC = new SuperColumn();
				 waldorfSC.setName(WALDORF_NAME.getBytes());
				 waldorfSC.columns = cols;

				 descCosc.super_column = waldorfSC;
				 columns.setColumn_or_supercolumn(descCosc);

				 columnsToAdd.add(columns);

				 String superCFName = "PointOfInterest";
				 ColumnPath cp = new ColumnPath();
				 cp.column_family = superCFName;
				 cp.setSuper_column(WALDORF_NAME.getBytes());
				 cp.setSuper_columnIsSet(true);

				 innerMap.put(superCFName, columnsToAdd);
				 outerMap.put(ByteBuffer.wrap(keyName.getBytes()), innerMap);
				 client.batch_mutate(outerMap, CL);

				 LOG.debug("Done inserting Central Park.");
	}

	private void insertPOIemireState() throws InvalidRequestException, UnavailableException, TimedOutException, TException, UnsupportedEncodingException {
		// TODO Auto-generated method stub
		Map<ByteBuffer, Map<String, List<Mutation>>> outerMap =
				 new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

				 List<Mutation> columnsToAdd = new ArrayList<Mutation>();

				 String esbName = "Empire State Building";
				 Column descCol = new Column();
				 descCol.setName("desc".getBytes(UTF8));
				 descCol.setValue("Great view from 102nd floor.".getBytes("UTF-8"));
				 Column phoneCol = new Column();
				 phoneCol.setName("phone".getBytes(UTF8));
				 phoneCol.setValue("212-777-7777".getBytes(UTF8));

				 List<Column> esbCols = new ArrayList<Column>();
				 esbCols.add(descCol);
				 esbCols.add(phoneCol);

				 Map<String, List<Mutation>> innerMap = new HashMap<String, List<Mutation>>();

				 Mutation columns = new Mutation();
				 ColumnOrSuperColumn descCosc = new ColumnOrSuperColumn();
				 SuperColumn waldorfSC = new SuperColumn();
				 waldorfSC.setName(WALDORF_NAME.getBytes());
				 waldorfSC.columns = esbCols;

				 descCosc.super_column = waldorfSC;
				 columns.setColumn_or_supercolumn(descCosc);

				 columnsToAdd.add(columns);

				 String superCFName = "PointOfInterest";
				 ColumnPath cp = new ColumnPath();
				 cp.column_family = superCFName;
				 cp.setSuper_column(WALDORF_NAME.getBytes());
				 cp.setSuper_columnIsSet(true);

				 innerMap.put(superCFName, columnsToAdd);
				 outerMap.put(ByteBuffer.wrap(esbName.getBytes()), innerMap);

				 client.batch_mutate(outerMap, CL);

				 LOG.debug("Done inserting Empire State.");
	}
}


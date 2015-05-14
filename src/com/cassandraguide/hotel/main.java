package com.cassandraguide.hotel;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.cassandraguide.hotel.*;
public class main {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Connector conn = new Connector();
		Prepolation p = new Prepolation();
		p.InsertCityIndexes();
	}

}

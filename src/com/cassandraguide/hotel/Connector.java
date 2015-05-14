package com.cassandraguide.hotel;

import static com.cassandraguide.hotel.Constants.KEYSPACE;
import static com.cassandraguide.hotel.Constants.HOST;
import static com.cassandraguide.hotel.Constants.PORT;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class Connector {
	public TTransport tr = new TSocket(HOST, PORT);

	public Cassandra.Client connect() throws TTransportException,
	TException, InvalidRequestException {
		TFramedTransport tf = new TFramedTransport(tr);
		TProtocol proto = new TBinaryProtocol(tf);
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		client.set_keyspace(KEYSPACE);
		return client;
	}

	public void close() {
		tr.close();
	}
}

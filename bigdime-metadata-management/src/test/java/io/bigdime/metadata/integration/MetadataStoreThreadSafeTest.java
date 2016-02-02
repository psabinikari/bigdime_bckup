/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.metadata.integration;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
//import io.bigdime.adaptor.metadata.impl.MetadataStoreImpl;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;

@ContextConfiguration(locations = "classpath:META-INF/application-context.xml")
public class MetadataStoreThreadSafeTest extends
		AbstractTestNGSpringContextTests {

	@Autowired
	MetadataStore metadataStore;

	@Test(priority = 30)
	public void threadSafeTest() {

		MyThread1 t1 = new MyThread1(metadataStore);
		MyThread2 t2 = new MyThread2(metadataStore);
		MyThread3 t3 = new MyThread3(metadataStore);
		MyThread4 t4 = new MyThread4(metadataStore);
		MyThread5 t5 = new MyThread5(metadataStore);
		// Create the entity UNIT_ENTITY11 for an adaptor: UnitAdaptor with schema type:
		// Hive
		t1.start();
		// t1.join();
		// Create the entity UNIT_ENTITY for an adaptor: UnitAdaptor with schema type:
		// Hive
		t2.start();
		// t2.join();
		// This is with less number of attributes for thread1.
		t3.start();
		// t3.join();
		// This is with less number and extra column of attributes for thread1.
		t4.start();
		// t4.join();
		// This is with less number of attributes for thread2.
		t5.start();
		// t5.join();

	}

}

class MyThread1 extends Thread {
	MetadataStore t;

	MyThread1(MetadataStore t) {
		this.t = t;
	}

	public void run() {

		Set<Attribute> AttributesSet = new LinkedHashSet<Attribute>();
		Attribute attribute = new Attribute();
		attribute.setAttributeName("USER_ID-1");
		attribute.setAttributeType("STRING");
		attribute.setIntPart("INTPART1");
		attribute.setFractionalPart("FRACTIONALPART1");
		attribute.setNullable("NO");
		attribute.setComment("Not Null");
		attribute.setFieldType("COLUMN");
		AttributesSet.add(attribute);
		Attribute attribute1 = new Attribute();
		attribute1.setAttributeName("dt-1");

		attribute1.setAttributeType("STRING");
		attribute1.setIntPart("INTPART2");
		attribute1.setFractionalPart("FRACTIONALPART2");
		attribute1.setNullable("NO");
		attribute1.setComment("Not Null");
		attribute1.setFieldType("PARTITIONCOLUMN");
		AttributesSet.add(attribute1);

		Set<Entitee> entitySet = new HashSet<Entitee>();
		Entitee entities = new Entitee();
		entities.setEntityName("UNIT_ENTITY11");
		entities.setEntityLocation("/data/UnitAdaptor/raw/UNIT_ENTITY1");
		entities.setDescription("HDFS LOCATION");
		entities.setVersion(1.1);

		entities.setAttributes(AttributesSet);

		entitySet.add(entities);

		Metasegment metasegment = new Metasegment();
		metasegment.setAdaptorName("UnitAdaptor");
		metasegment.setSchemaType("HIVE");
		metasegment.setEntitees(entitySet);
		metasegment.setDatabaseName("UNITDB");
		metasegment.setDatabaseLocation("");
		metasegment.setRepositoryType("TARGET");
		metasegment.setIsDataSource("Y");

		metasegment.setCreatedBy("TEST_USER");
		metasegment.setUpdatedBy("TEST_USER");
		try {
			t.put(metasegment);
		} catch (MetadataAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}

class MyThread2 extends Thread {
	MetadataStore t;

	MyThread2(MetadataStore t) {
		this.t = t;
	}

	public void run() {

		Set<Attribute> AttributesSet = new LinkedHashSet<Attribute>();
		Attribute attribute = new Attribute();
		attribute.setAttributeName("USER_ID-2");
		attribute.setAttributeType("STRING");
		attribute.setIntPart("INTPART1");
		attribute.setFractionalPart("FRACTIONALPART1");
		attribute.setNullable("NO");
		attribute.setComment("Not Null");
		attribute.setFieldType("COLUMN");
		AttributesSet.add(attribute);
		Attribute attribute0 = new Attribute();
		attribute0.setAttributeName("Test-3");
		attribute0.setAttributeType("STRING");
		attribute0.setIntPart("INTPART1");
		attribute0.setFractionalPart("FRACTIONALPART1");
		attribute0.setNullable("NO");
		attribute0.setComment("Not Null");
		attribute0.setFieldType("COLUMN");
		AttributesSet.add(attribute0);
		Attribute attribute1 = new Attribute();
		attribute1.setAttributeName("dt-2");

		attribute1.setAttributeType("STRING");
		attribute1.setIntPart("INTPART2");
		attribute1.setFractionalPart("FRACTIONALPART2");
		attribute1.setNullable("NO");
		attribute1.setComment("Not Null");
		attribute1.setFieldType("PARTITIONCOLUMN");
		AttributesSet.add(attribute1);

		Set<Entitee> entitySet = new HashSet<Entitee>();
		Entitee entities = new Entitee();
		entities.setEntityName("UNIT_ENTITY");
		entities.setEntityLocation("/data/UnitAdaptor/raw/UNIT_ENTITY1");
		entities.setDescription("HDFS LOCATION");
		entities.setVersion(1.1);

		entities.setAttributes(AttributesSet);

		entitySet.add(entities);

		Metasegment metasegment = new Metasegment();
		metasegment.setAdaptorName("UnitAdaptor");
		metasegment.setSchemaType("HIVE");
		metasegment.setEntitees(entitySet);
		metasegment.setDatabaseName("UNITDB");
		metasegment.setDatabaseLocation("");
		metasegment.setRepositoryType("TARGET");
		metasegment.setIsDataSource("Y");

		metasegment.setCreatedBy("TEST_USER");
		metasegment.setUpdatedBy("TEST_USER");
		try {
			t.put(metasegment);
		} catch (MetadataAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

class MyThread3 extends Thread {
	MetadataStore t;

	MyThread3(MetadataStore t) {
		this.t = t;
	}

	public void run() {

		Set<Attribute> AttributesSet = new LinkedHashSet<Attribute>();
		Attribute attribute = new Attribute();
		attribute.setAttributeName("USER_ID-1");
		attribute.setAttributeType("STRING");
		attribute.setIntPart("INTPART1");
		attribute.setFractionalPart("FRACTIONALPART1");
		attribute.setNullable("NO");
		attribute.setComment("Not Null");
		attribute.setFieldType("COLUMN");
		AttributesSet.add(attribute);

		Set<Entitee> entitySet = new HashSet<Entitee>();
		Entitee entities = new Entitee();
		entities.setEntityName("UNIT_ENTITY11");
		entities.setEntityLocation("/data/UnitAdaptor/raw/UNIT_ENTITY1");
		entities.setDescription("HDFS LOCATION");
		entities.setVersion(1.1);

		entities.setAttributes(AttributesSet);

		entitySet.add(entities);

		Metasegment metasegment = new Metasegment();
		metasegment.setAdaptorName("UnitAdaptor");
		metasegment.setSchemaType("HIVE");
		metasegment.setEntitees(entitySet);
		metasegment.setDatabaseName("UNITDB");
		metasegment.setDatabaseLocation("");
		metasegment.setRepositoryType("TARGET");
		metasegment.setIsDataSource("Y");

		metasegment.setCreatedBy("TEST_USER");
		metasegment.setUpdatedBy("TEST_USER");
		try {
			t.put(metasegment);
		} catch (MetadataAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}

class MyThread4 extends Thread {
	MetadataStore t;

	MyThread4(MetadataStore t) {
		this.t = t;
	}

	public void run() {

		Set<Attribute> AttributesSet = new LinkedHashSet<Attribute>();
		Attribute attribute = new Attribute();
		attribute.setAttributeName("USER_ID-1");
		attribute.setAttributeType("STRING");
		attribute.setIntPart("INTPART1");
		attribute.setFractionalPart("FRACTIONALPART1");
		attribute.setNullable("NO");
		attribute.setComment("Not Null");
		attribute.setFieldType("COLUMN");
		AttributesSet.add(attribute);

		Attribute attribute1 = new Attribute();
		attribute1.setAttributeName("TestExtraColumn");
		attribute1.setAttributeType("STRING");
		attribute1.setIntPart("INTPART1");
		attribute1.setFractionalPart("FRACTIONALPART1");
		attribute1.setNullable("NO");
		attribute1.setComment("Not Null");
		attribute1.setFieldType("COLUMN");
		AttributesSet.add(attribute1);

		Set<Entitee> entitySet = new HashSet<Entitee>();
		Entitee entities = new Entitee();
		entities.setEntityName("UNIT_ENTITY11");
		entities.setEntityLocation("/data/UnitAdaptor/raw/UNIT_ENTITY1");
		entities.setDescription("HDFS LOCATION");
		entities.setVersion(1.1);

		entities.setAttributes(AttributesSet);

		entitySet.add(entities);

		Metasegment metasegment = new Metasegment();
		metasegment.setAdaptorName("UnitAdaptor");
		metasegment.setSchemaType("HIVE");
		metasegment.setEntitees(entitySet);
		metasegment.setDatabaseName("UNITDB");
		metasegment.setDatabaseLocation("");
		metasegment.setRepositoryType("TARGET");
		metasegment.setIsDataSource("Y");

		metasegment.setCreatedBy("TEST_USER");
		metasegment.setUpdatedBy("TEST_USER");
		try {
			t.put(metasegment);
		} catch (MetadataAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}

class MyThread5 extends Thread {
	MetadataStore t;

	MyThread5(MetadataStore t) {
		this.t = t;
	}

	public void run() {

		Set<Attribute> AttributesSet = new LinkedHashSet<Attribute>();
		Attribute attribute = new Attribute();
		attribute.setAttributeName("USER_ID-2");
		attribute.setAttributeType("STRING");
		attribute.setIntPart("INTPART1");
		attribute.setFractionalPart("FRACTIONALPART1");
		attribute.setNullable("NO");
		attribute.setComment("Not Null");
		attribute.setFieldType("COLUMN");
		AttributesSet.add(attribute);

		Attribute attribute1 = new Attribute();
		attribute1.setAttributeName("dt-2");

		attribute1.setAttributeType("STRING");
		attribute1.setIntPart("INTPART2");
		attribute1.setFractionalPart("FRACTIONALPART2");
		attribute1.setNullable("NO");
		attribute1.setComment("Not Null");
		attribute1.setFieldType("PARTITIONCOLUMN");
		AttributesSet.add(attribute1);

		Set<Entitee> entitySet = new HashSet<Entitee>();
		Entitee entities = new Entitee();
		entities.setEntityName("UNIT_ENTITY");
		entities.setEntityLocation("/data/UnitAdaptor/raw/UNIT_ENTITY1");
		entities.setDescription("HDFS LOCATION");
		entities.setVersion(1.1);

		entities.setAttributes(AttributesSet);

		entitySet.add(entities);

		Metasegment metasegment = new Metasegment();
		metasegment.setAdaptorName("UnitAdaptor");
		metasegment.setSchemaType("HIVE");
		metasegment.setEntitees(entitySet);
		metasegment.setDatabaseName("UNITDB");
		metasegment.setDatabaseLocation("");
		metasegment.setRepositoryType("TARGET");
		metasegment.setIsDataSource("Y");

		metasegment.setCreatedBy("TEST_USER");
		metasegment.setUpdatedBy("TEST_USER");
		try {
			t.put(metasegment);
		} catch (MetadataAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}

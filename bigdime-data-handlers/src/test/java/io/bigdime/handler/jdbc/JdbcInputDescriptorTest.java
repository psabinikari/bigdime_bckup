package io.bigdime.handler.jdbc;

//import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.common.testutils.GetterSetterTestHelper;

import java.lang.reflect.Field;

import org.apache.commons.lang3.reflect.FieldUtils;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockito.Mockito.*;

import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class JdbcInputDescriptorTest {
	
	//@Mock
	JdbcInputDescriptor jdbcInputDescriptor;
	
	@BeforeClass
	public void init() {
		jdbcInputDescriptor = new JdbcInputDescriptor();
		initMocks(this);
	}
	
	@Test
	public void testQueryFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "query", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testEntityNameFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "entityName", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	@Test
	public void testIncrementedByFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "incrementedBy", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testIncrementedColumnTypeFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "incrementedColumnType", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	
	@Test
	public void testColumnNameFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "columnName", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
		
	}
	
	@Test
	public void testPartitionFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "partition", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testFieldDelimeterFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "fieldDelimeter", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testRowDelimeterFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "rowDelimeter", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	
	@Test
	public void testSnapshotFieldGettersAndSetters() {
		Field field = FieldUtils.getField(JdbcInputDescriptor.class, "snapshot", true);
		GetterSetterTestHelper.doTest(jdbcInputDescriptor, field.getName(),
				"UNIT-TEST-" + field.getName());
		
	}
	
	@Test
	public void testFormatQueryWithoutQueryParameter() throws JdbcHandlerException{
		ReflectionTestUtils.setField(jdbcInputDescriptor, "query", "testQuery");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "incrementedBy", "testIncrementedBy");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "splitSize", "testSplitSize");
		Assert.assertNotNull(jdbcInputDescriptor.formatQuery("testDrviverName"));
	}
	
	@Test
	public void testFormatQueryWithOrderBy() throws JdbcHandlerException {
		ReflectionTestUtils.setField(jdbcInputDescriptor, "query", "testQuery ? testIncrementedBy");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "incrementedBy", "testIncrementedBy");
		Assert.assertNotNull(jdbcInputDescriptor.formatQuery("testDrviverName"));
	}
	
	
	@Test
	public void testFormatQuerySplitSize() throws JdbcHandlerException {
		
		ReflectionTestUtils.setField(jdbcInputDescriptor, "query", "testQuery ? testIncrementedBy ORDER BY");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "incrementedBy", "testIncrementedBy");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "splitSize", "12");
		Assert.assertNotNull(jdbcInputDescriptor.formatQuery("testOracleDriver"));
	}
	
	@Test(expectedExceptions=JdbcHandlerException.class)
	public void testFormatQueryException() throws JdbcHandlerException {
		ReflectionTestUtils.setField(jdbcInputDescriptor, "query", "testQuery ?");
		ReflectionTestUtils.setField(jdbcInputDescriptor, "incrementedBy", "testIncrementedBy");
		Assert.assertNotNull(jdbcInputDescriptor.formatQuery("testDrviverName"));
	}
	
	@Test
	public void testParseDescriptor(){
		jdbcInputDescriptor.parseDescriptor("{\"query\":\"select p.* from testTableName\", \"incrementedBy\": \"DOB\", \"partitionedColumns\": \"id\"}");
		Assert.assertNotNull(jdbcInputDescriptor.getQuery());
	}
	
	@Test(expectedExceptions=IllegalArgumentException.class)
	public void testParseDescriptorException() {
		jdbcInputDescriptor.parseDescriptor(null);
		
	}

}

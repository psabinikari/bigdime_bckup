/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.libs.hive.common.Column;
import io.bigdime.libs.hive.metadata.TableMetaData;
import io.bigdime.libs.hive.table.HiveTableManger;

import org.apache.hive.hcatalog.common.HCatException;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

@PrepareForTest(HiveTableManger.class)
public class SourceColumnTypeValidatorTest extends PowerMockTestCase {
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHiveHostTest() throws DataValidationException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("");
		columnTypeValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHivePortNumberTest() throws DataValidationException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("hiveHost").thenReturn(null);
		columnTypeValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = NumberFormatException.class)
	public void validatePortNumberFormatTest() throws DataValidationException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		headers.put(ActionEventHeaderConstants.HIVE_PORT, "port");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		columnTypeValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullDBNameTest() throws DataValidationException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		headers.put(ActionEventHeaderConstants.HIVE_PORT, "1234");
		headers.put(ActionEventHeaderConstants.HIVE_DB_NAME, null);
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		columnTypeValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullEntityNameTest() throws DataValidationException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		headers.put(ActionEventHeaderConstants.HIVE_PORT, "1234");
		headers.put(ActionEventHeaderConstants.HIVE_DB_NAME, "mockDB");
		headers.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "");
		when(mockActionEvent.getHeaders()).thenReturn(headers);
		columnTypeValidator.validate(mockActionEvent);
	}
	
	@Test
	public void validateHiveTableNotCreated() throws DataValidationException, HCatException{
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		when(mockActionEvent.getHeaders()).thenReturn(setCommonParameters(mockActionEvent));
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(false);
		Assert.assertEquals(columnTypeValidator.validate(mockActionEvent).getValidationResult(), ValidationResult.INCOMPLETE_SETUP);
	}
	
	@Test
	public void validateEntityNotFound() throws DataValidationException, HCatException, MetadataAccessException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		when(mockActionEvent.getHeaders()).thenReturn(setCommonParameters(mockActionEvent));
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		TableMetaData mockTable = Mockito.mock(TableMetaData.class);
		Mockito.when(mockHiveTableManager.getTableMetaData(anyString(), anyString())).thenReturn(mockTable);
		MetadataStore mockMetadataStore = Mockito.mock(MetadataStore.class);
		ReflectionTestUtils.setField(columnTypeValidator, "metadataStore", mockMetadataStore);
		List<Column> hiveColumns = new ArrayList<Column>();
		Mockito.when(mockTable.getColumns()).thenReturn(hiveColumns);
		Mockito.when(mockMetadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(null);
		Assert.assertEquals(columnTypeValidator.validate(mockActionEvent).getValidationResult(), ValidationResult.INCOMPLETE_SETUP);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void validateColumnTypeMatchWithSameColumnCount() throws DataValidationException, HCatException, MetadataAccessException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		when(mockActionEvent.getHeaders()).thenReturn(setCommonParameters(mockActionEvent));
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		TableMetaData mockTable = Mockito.mock(TableMetaData.class);
		Mockito.when(mockHiveTableManager.getTableMetaData(anyString(), anyString())).thenReturn(mockTable);
		Mockito.when(mockTable.getColumns()).thenReturn(createHiveTableWithTwoColumns());
		MetadataStore mockMetadataStore = Mockito.mock(MetadataStore.class);
		ReflectionTestUtils.setField(columnTypeValidator, "metadataStore", mockMetadataStore);
		Metasegment mockMetasegment = Mockito.mock(Metasegment.class);
		Mockito.when(mockMetadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(mockMetasegment);
		Set<Entitee> mockEntitySet = Mockito.mock(Set.class);
		Mockito.when(mockMetasegment.getEntitees()).thenReturn(mockEntitySet);
		Mockito.when(mockEntitySet.size()).thenReturn(Integer.valueOf(1));
		Entitee mockEntitee = Mockito.mock(Entitee.class);
		Mockito.when(mockMetasegment.getEntity(anyString())).thenReturn(mockEntitee);
		Mockito.when(mockEntitee.getAttributes()).thenReturn(createEntityWithTwoAttributes());
		Assert.assertEquals(columnTypeValidator.validate(mockActionEvent).getValidationResult(), ValidationResult.PASSED);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void validateColumnTypeMismatchSourceColumnMore() throws DataValidationException, HCatException, MetadataAccessException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		when(mockActionEvent.getHeaders()).thenReturn(setCommonParameters(mockActionEvent));
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		TableMetaData mockTable = Mockito.mock(TableMetaData.class);
		Mockito.when(mockHiveTableManager.getTableMetaData(anyString(), anyString())).thenReturn(mockTable);
		Mockito.when(mockTable.getColumns()).thenReturn(createHiveTableWithTwoColumns());
		MetadataStore mockMetadataStore = Mockito.mock(MetadataStore.class);
		ReflectionTestUtils.setField(columnTypeValidator, "metadataStore", mockMetadataStore);
		Metasegment mockMetasegment = Mockito.mock(Metasegment.class);
		Mockito.when(mockMetadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(mockMetasegment);
		Set<Entitee> mockEntitySet = Mockito.mock(Set.class);
		Mockito.when(mockMetasegment.getEntitees()).thenReturn(mockEntitySet);
		Mockito.when(mockEntitySet.size()).thenReturn(Integer.valueOf(1));
		Entitee mockEntitee = Mockito.mock(Entitee.class);
		Mockito.when(mockMetasegment.getEntity(anyString())).thenReturn(mockEntitee);
		Mockito.when(mockEntitee.getAttributes()).thenReturn(createEntityWithThreeAttributes());
		Assert.assertEquals(columnTypeValidator.validate(mockActionEvent).getValidationResult(), ValidationResult.COLUMN_TYPE_MISMATCH);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void validateColumnTypeMismatchHiveColumnMore() throws DataValidationException, HCatException, MetadataAccessException {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		when(mockActionEvent.getHeaders()).thenReturn(setCommonParameters(mockActionEvent));
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		TableMetaData mockTable = Mockito.mock(TableMetaData.class);
		Mockito.when(mockHiveTableManager.getTableMetaData(anyString(), anyString())).thenReturn(mockTable);
		Mockito.when(mockTable.getColumns()).thenReturn(createHiveTableWithThreeColumns());
		MetadataStore mockMetadataStore = Mockito.mock(MetadataStore.class);
		ReflectionTestUtils.setField(columnTypeValidator, "metadataStore", mockMetadataStore);
		Metasegment mockMetasegment = Mockito.mock(Metasegment.class);
		Mockito.when(mockMetadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(mockMetasegment);
		Set<Entitee> mockEntitySet = Mockito.mock(Set.class);
		Mockito.when(mockMetasegment.getEntitees()).thenReturn(mockEntitySet);
		Mockito.when(mockEntitySet.size()).thenReturn(Integer.valueOf(1));
		Entitee mockEntitee = Mockito.mock(Entitee.class);
		Mockito.when(mockMetasegment.getEntity(anyString())).thenReturn(mockEntitee);
		Mockito.when(mockEntitee.getAttributes()).thenReturn(createEntityWithTwoAttributes());
		Assert.assertEquals(columnTypeValidator.validate(mockActionEvent).getValidationResult(), ValidationResult.COLUMN_TYPE_MISMATCH);
	}
	
	@Test
	public void gettersAndSettersTest() {
		ColumnTypeValidator columnTypeValidator = new ColumnTypeValidator();
		columnTypeValidator.setName("testName");
		Assert.assertEquals(columnTypeValidator.getName(), "testName");
	}
	
	private Map<String, String> setCommonParameters(ActionEvent actionEvent){
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.HIVE_HOST_NAME, "host");
		headers.put(ActionEventHeaderConstants.HIVE_PORT, "1234");
		headers.put(ActionEventHeaderConstants.HIVE_DB_NAME, "mockDB");
		headers.put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "mockTable");
		return headers;
	}
	
	private List<Column> createHiveTableWithTwoColumns() {
		List<Column> hiveColumns = new ArrayList<Column>();
		Column column = new Column("id", "int", "comment");
		hiveColumns.add(column);
		List<Column> partitionColumns = new ArrayList<Column>();
		Column parColumn = new Column("dt", "String", "partition column");
		partitionColumns.add(parColumn);
		hiveColumns.addAll(partitionColumns);
		return hiveColumns;
	}
	
	private List<Column> createHiveTableWithThreeColumns() {
		List<Column> hiveColumns = new ArrayList<Column>();
		Column column = new Column("id", "int", "comment");
		hiveColumns.add(column);
		column = new Column("name", "varchar", "comment");
		hiveColumns.add(column);
		List<Column> partitionColumns = new ArrayList<Column>();
		Column parColumn = new Column("dt", "String", "partition column");
		partitionColumns.add(parColumn);
		hiveColumns.addAll(partitionColumns);
		return hiveColumns;
	}
	
	private Set<Attribute> createEntityWithTwoAttributes() {
		Set<Attribute> metaColumns = new LinkedHashSet<Attribute>();
		Attribute attr1 = new Attribute();
		attr1.setAttributeName("id");
		attr1.setAttributeType("int");
		metaColumns.add(attr1);
		Attribute attr2 = new Attribute();
		attr2.setAttributeName("dt");
		attr2.setAttributeType("string");
		metaColumns.add(attr2);
		return metaColumns;
	}
	
	private Set<Attribute> createEntityWithThreeAttributes() {
		Set<Attribute> metaColumns = new LinkedHashSet<Attribute>();
		Attribute attr1 = new Attribute();
		attr1.setAttributeName("id");
		attr1.setAttributeType("int");
		metaColumns.add(attr1);
		Attribute attr2 = new Attribute();
		attr2.setAttributeName("name");
		attr2.setAttributeType("string");
		metaColumns.add(attr2);
		Attribute attr3 = new Attribute();
		attr3.setAttributeName("dt");
		attr3.setAttributeType("string");
		metaColumns.add(attr3);
		return metaColumns;
	}
}

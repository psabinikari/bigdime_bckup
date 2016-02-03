/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.validation.DataValidationConstants;
import io.bigdime.validation.RecordCountValidator;
import io.bigdime.libs.hdfs.WebHdfs;
import io.bigdime.libs.hive.table.HiveTableManger;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.testng.Assert;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@PrepareForTest(HiveTableManger.class)
public class RecordCountValidatorTest extends PowerMockTestCase {

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHiveHostTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("");
		recordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHivePortTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn(null);
		recordCountValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = NumberFormatException.class)
	public void validatePortNumberFormatTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("port");
		recordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullSrcRecordCountTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("");
		recordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = NumberFormatException.class)
	public void validateSrcRCParseToIntTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("src");
		recordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHiveDBNameTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("2342").thenReturn("");
		recordCountValidator.validate(mockActionEvent);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullHiveTableNameTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("453").thenReturn("db")
				.thenReturn(null);
		recordCountValidator.validate(mockActionEvent);
	}
	
	@Test(expectedExceptions = DataValidationException.class)
	public void validateHiveTableNotCreated() throws DataValidationException, HCatException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("453").thenReturn("db")
				.thenReturn("table");
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(false);
		recordCountValidator.validate(mockActionEvent);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void recordCountWithoutPartitionDiffTest()
			throws DataValidationException, ClientProtocolException, IOException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("2342")
				.thenReturn("db").thenReturn("table").thenReturn(null).thenReturn("").thenReturn("false");
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		ReaderContext mockContext = Mockito.mock(ReaderContext.class);
		Mockito.when(mockHiveTableManager.readData(anyString(), anyString(), anyString(), anyString(), anyInt(), anyMap()))
				.thenReturn(mockContext);
		Assert.assertEquals(recordCountValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void recordCountWithPartitionMatchTest()
			throws DataValidationException, ClientProtocolException, IOException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = Mockito.mock(Map.class);
		String hivePartitionsNames = "dt, hr";
		String hivePartitionValues = "20120202, 01";

		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("0").thenReturn("db")
				.thenReturn("table").thenReturn(hivePartitionsNames).thenReturn(hivePartitionValues).thenReturn("false");;
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		ReaderContext mockContext = Mockito.mock(ReaderContext.class);
		Mockito.when(mockHiveTableManager.readData(anyString(), anyString(), anyString(), anyString(), anyInt(), anyMap()))
				.thenReturn(mockContext);
		Assert.assertEquals(recordCountValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.PASSED);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void recordCountWithHAEnabledDiffTest()
			throws DataValidationException, ClientProtocolException, IOException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("2342")
				.thenReturn("db").thenReturn("table").thenReturn("").thenReturn(null)
				.thenReturn("true").thenReturn("dfs proxy").thenReturn("haServiceName")
				.thenReturn("dfs service").thenReturn("dfs namenode 1").thenReturn("dfs namenode 2");
		HiveTableManger mockHiveTableManager = Mockito.mock(HiveTableManger.class);
		PowerMockito.mockStatic(HiveTableManger.class);
		PowerMockito.when(HiveTableManger.getInstance((Properties) Mockito.any())).thenReturn(mockHiveTableManager);	
		Mockito.when(mockHiveTableManager.isTableCreated(anyString(), anyString())).thenReturn(true);
		ReaderContext mockContext = Mockito.mock(ReaderContext.class);
		Mockito.when(mockHiveTableManager.readData(anyString(), anyString(), anyString(), anyString(), anyInt(), anyMap()))
				.thenReturn(mockContext);
		Assert.assertEquals(recordCountValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
	}

	@Test
	public void getInstanceTest() {
		Assert.assertNotNull(DataValidationConstants.getInstance());
	}

	@Test
	public void gettersAndSettersTest() {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		recordCountValidator.setName("testName");
		Assert.assertEquals(recordCountValidator.getName(), "testName");
	}

}

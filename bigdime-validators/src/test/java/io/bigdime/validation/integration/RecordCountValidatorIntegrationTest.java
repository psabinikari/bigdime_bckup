/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.libs.hive.constants.HiveClientConstants;
import io.bigdime.validation.RecordCountValidator;

public class RecordCountValidatorIntegrationTest{
	
	private static final Logger logger = LoggerFactory.getLogger(RecordCountValidatorIntegrationTest.class);

    @BeforeTest
	public void setup() {
		logger.info("Setting the environment");
	}

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullHiveHostName() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RecordCountValidator recordCountValidator= new RecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "");
    	recordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullPort() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RecordCountValidator recordCountValidator= new RecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "");
    	recordCountValidator.validate(actionEvent);
    }
  
    @Test(expectedExceptions = NumberFormatException.class)
    public void testParsePortStringToIntException() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RecordCountValidator recordCountValidator= new RecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "port");
    	recordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullSrcRecordCount() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RecordCountValidator recordCountValidator= new RecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, null);
    	recordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = NumberFormatException.class)
    public void testParseSrcRecordCountStringToInt() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RecordCountValidator recordCountValidator= new RecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "count");
    	recordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullHiveDBName() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RecordCountValidator recordCountValidator= new RecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "");
    	recordCountValidator.validate(actionEvent);
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullHiveTableName() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RecordCountValidator recordCountValidator= new RecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "123");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "");
    	recordCountValidator.validate(actionEvent);
    }
 
    @Test
    public void testValidateRecordCountWithPartitionsDiff() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RecordCountValidator recordCountValidator= new RecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "234");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "two_test");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_NAMES, "entityName, dt");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "two, 20120218");
    	actionEvent.getHeaders().put(HiveClientConstants.HA_ENABLED, "false");
    	Assert.assertEquals(recordCountValidator.validate(actionEvent).getValidationResult(), ValidationResult.FAILED);
    }
    
    @Test
    public void testValidateRecordCountWithPartitionsHaEnabled() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RecordCountValidator recordCountValidator= new RecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "234");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "two_test");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_NAMES, "entityName, dt");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "two, 20120218");
    	actionEvent.getHeaders().put(HiveClientConstants.HA_ENABLED, "true");
    	actionEvent.getHeaders().put(HiveClientConstants.HA_SERVICE_NAME, "");
    	actionEvent.getHeaders().put(HiveClientConstants.DFS_CLIENT_FAILOVER_PROVIDER, "");
    	actionEvent.getHeaders().put(HiveClientConstants.DFS_NAME_SERVICES, "");
    	actionEvent.getHeaders().put(HiveClientConstants.DFS_HA_NAMENODES, "");
    	actionEvent.getHeaders().put(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE1, "");
    	actionEvent.getHeaders().put(HiveClientConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE2, "");
    	Assert.assertEquals(recordCountValidator.validate(actionEvent).getValidationResult(), ValidationResult.FAILED);
    }
    
    @Test
    public void testValidateRecordCountWithoutPartitions() throws DataValidationException{
    	ActionEvent actionEvent = new ActionEvent();
    	RecordCountValidator recordCountValidator= new RecordCountValidator();
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_RECORD_COUNT, "3");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
    	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "one");
    	Assert.assertEquals(recordCountValidator.validate(actionEvent).getValidationResult(), ValidationResult.PASSED);
    }
    
	@Test
	public void testSettersAndGetters(){
		RecordCountValidator recordCountValidator= new RecordCountValidator();
		recordCountValidator.setName("testName");
		Assert.assertEquals(recordCountValidator.getName(), "testName");
	}
}

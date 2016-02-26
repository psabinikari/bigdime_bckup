/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.hive;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.hcatalog.api.ObjectNotFoundException;
import org.apache.hive.hcatalog.common.HCatException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.libs.hive.common.Column;
import io.bigdime.libs.hive.database.DatabaseSpecification;
import io.bigdime.libs.hive.database.HiveDBManger;
import io.bigdime.libs.hive.partition.HivePartitionManger;
import io.bigdime.libs.hive.partition.PartitionSpecification;
import io.bigdime.libs.hive.table.HiveTableManger;
import io.bigdime.libs.hive.table.TableSpecification;
/**
 * HiveMetaDataHandler functionality is to create table and database and partions.
 * in case of exceptions from hive simply log and send them alerts.
 * @author mnamburi
 *
 */
@Component
@Scope("prototype")
public class HiveMetaDataHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(HiveMetaDataHandler.class));
	private static final char default_char = '\u0000';
	private String handlerPhase = "building HiveMetaDataHandler";
	private String webhdfs_scheme = "/webhdfs/v1";
	private Properties props = new Properties();
	//private HiveDBManger hiveDBManager = null;
	//private HiveTableManger hiveTableManager = null;
	private HivePartitionManger hivePartitionManager = null;
	private static String hdfsScheme = null;
	
	@Autowired private MetadataStore metadataStore;

	@Override
	public void build() throws AdaptorConfigurationException {
		props.putAll(getPropertyMap());
		hdfsScheme = PropertyHelper.getStringProperty(getPropertyMap(), "hive.uri.hdfs.scheme","hdfs://");
	}

	@Override
	public Status process() throws HandlerException {
		try {
			handlerPhase = "process HiveMetaDataHandler";
			logger.debug(handlerPhase, "process HiveMetaDataHandler");
			
			List<ActionEvent> actionEvents = getHandlerContext().getEventList();
			Preconditions.checkNotNull(actionEvents, "eventList in HandlerContext can't be null");
			Preconditions.checkArgument(!actionEvents.isEmpty(), "eventList in HandlerContext can't be empty");
			ActionEvent actionEvent = actionEvents.get(0);
			String entityName = actionEvent.getHeaders().get(ActionEventHeaderConstants.ENTITY_NAME);
			Preconditions.checkNotNull(entityName,"EntityName cannot be null");

			Metasegment metasegment = metadataStore.getAdaptorMetasegment(AdaptorConfig.getInstance().getName(), "HIVE", entityName);
			Entitee entitee = metasegment.getEntity(entityName);

			String partitionKeys = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_NAMES);
			String partitionValues = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_VALUES);
			String partitionLocation = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_LOCATION);
			String hdfsBasePath = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_PATH);
			
			actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, metasegment.getDatabaseName());
			actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, entityName);

			createDatabase(metasegment);
			createTable(metasegment.getDatabaseName(),entitee,actionEvent);

			if(partitionKeys != null){
				HashMap<String,String> partitionMap = getPartitionsMap(partitionKeys, partitionValues);
				Preconditions.checkNotNull(partitionValues,"Partition Values cannot be null");
				if(partitionLocation == null)
					partitionLocation  = getCompletePartitionPath(hdfsBasePath + getHiveNonPartitionValues(actionEvent),partitionValues);
				Preconditions.checkNotNull(partitionLocation,"Partition Location cannot be null");
				createPartition(metasegment.getDatabaseName(), entitee.getEntityName(),partitionMap,partitionLocation);
			}
		} catch (MetadataAccessException e) {
			logger.alert(ALERT_TYPE.OTHER_ERROR, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
					"\"hive metadata handler exception \" error={}", e);			
			throw new HandlerException(e);
		} catch (HCatException e) {
			logger.alert(ALERT_TYPE.OTHER_ERROR, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
					"\"hive metadata handler exception \" error={}", e);			
			// TODO : in case of exceptions from hive simply log and send them alerts.
		} catch (Exception e) {
			logger.alert(ALERT_TYPE.OTHER_ERROR, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
					"\"hive metadata handler exception \" error={}", e);
			throw new HandlerException(e);
		}
		return Status.READY;
	}

	/**
	 * @throws HCatException 
	 * 
	 */
	private synchronized void createDatabase(Metasegment metasegment) throws HCatException {
		HiveDBManger hiveDBManager = HiveDBManger.getInstance(props);
		DatabaseSpecification.Builder databaseSpecificationBuilder = new DatabaseSpecification.Builder(metasegment.getDatabaseName());
		DatabaseSpecification  databaseSpecification = databaseSpecificationBuilder.location(metasegment.getDatabaseLocation()).scheme(hdfsScheme).build();
		try {
			if(!hiveDBManager.isDatabaseCreated(metasegment.getDatabaseName())){
				hiveDBManager.createDatabase(databaseSpecification);
			}
		} catch (HCatException e) {
			logger.alert(ALERT_TYPE.OTHER_ERROR, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.MAJOR,
					"\"hive db creation failed \" database ={} error={}", metasegment.getDatabaseName(),e.toString());
					throw e;
		}
	}
	/**
	 * 
	 * @param dbName
	 * @param entitee
	 * @param actionEvent
	 * @throws HCatException
	 */
	private synchronized void createTable(String dbName,Entitee entitee,ActionEvent actionEvent) throws HCatException {
		if(isTableCreated(dbName,entitee.getEntityName())){
			return;
		}
		HiveTableManger hiveTableManager = HiveTableManger.getInstance(props);
		List<Column> columns = new ArrayList<Column>();
		
		Column column = null;
		//String hiveTableLocation = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_TABLE_LOCATION);
		String hdfsPath = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_PATH);
		String hiveNonPartitionValues = getHiveNonPartitionValues(actionEvent);
		String hiveTableLocation = StringUtils.remove(hdfsPath, webhdfs_scheme) + hiveNonPartitionValues;
		String fieldsTerminatedBy = actionEvent.getHeaders().get(ActionEventHeaderConstants.FIELDS_TERMINATED_BY);
		String linesTerminatedBy = actionEvent.getHeaders().get(ActionEventHeaderConstants.LINES_TERMINATED_BY);
		String partitionKeys = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_NAMES);
		char fieldsTerminated = default_char;
		char linesTerminated = default_char;

		if(fieldsTerminatedBy != null){
			fieldsTerminated = fieldsTerminatedBy.toCharArray()[0];
		}

		if(linesTerminatedBy != null){
			linesTerminated = linesTerminatedBy.toCharArray()[0];
		}

		TableSpecification.Builder tableSpecBuilder = new TableSpecification.Builder(dbName, entitee.getEntityName());


		Set<Attribute> attributes = entitee.getAttributes();
		Preconditions.checkNotNull(attributes,"Attubutes cannot be null");
		for(Attribute attribute : attributes){
			column = new Column(attribute.getAttributeName(), attribute.getAttributeType(), attribute.getComment());
			columns.add(column);
		}

		TableSpecification  tableSpec = tableSpecBuilder.externalTableLocation(hiveTableLocation)
				.columns(columns)
				.fieldsTerminatedBy(fieldsTerminated)
				.linesTerminatedBy(linesTerminated)
				.partitionColumns(getPartitionsColumns(partitionKeys))
				.build();
		try {
			hiveTableManager.createTable(tableSpec);
		} catch (HCatException e) {
			logger.alert(ALERT_TYPE.OTHER_ERROR, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.MAJOR,
					"\"hive table creation failed \" database ={} tableName={} columnsSize = {} error={}", dbName,entitee.getEntityName(),columns.size(), e.toString());
					throw e;
		}
	}
	
	private String getHiveNonPartitionValues(ActionEvent actionEvent){
		String hiveNonPartitionNames = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_NON_PARTITION_NAMES);
		StringBuffer hiveNonPartitionValuessb = new StringBuffer();
		if(hiveNonPartitionNames != null){
			String[] hiveNonPartitionStNames = hiveNonPartitionNames.split(",");
	        for(String hiveNonPartitionName: hiveNonPartitionStNames){	
	        	hiveNonPartitionValuessb.append(actionEvent.getHeaders().get(hiveNonPartitionName));
	        	hiveNonPartitionValuessb.append("/");
	        }
	        
	        	}
		
		if(hiveNonPartitionValuessb.length() > 1)
		return hiveNonPartitionValuessb.toString();
		
		return null;
	        	
	}
	
	/**
	 * 
	 * @param databaseName
	 * @param tableName
	 * @return
	 */
	private boolean isTableCreated(String databaseName,String tableName) {
		boolean isCreated = false;
		HiveTableManger hiveTableManager = HiveTableManger.getInstance(props);

		try {
			hiveTableManager.getTableMetaData(databaseName, tableName);
			isCreated = true;
		} catch (HCatException e) {
			if (ObjectNotFoundException.class == e.getClass()) {
				isCreated = false;
			}
		}
		return isCreated;
	
	}
	/**
	 * 
	 * @param dbName
	 * @param tableName
	 * @param partitionMap
	 * @param location
	 * @throws HCatException 
	 * @throws Exception
	 */
	private void createPartition(String dbName,String tableName,HashMap<String,String> partitionMap,String location) throws HCatException{
		hivePartitionManager = HivePartitionManger.getInstance(props);
		PartitionSpecification.Builder partitionSpecificationBuilder = new PartitionSpecification.Builder(dbName, tableName);

		PartitionSpecification partitionSpecification = partitionSpecificationBuilder
				.location(location)
				.partitionColumns(partitionMap)
				.build();
		try {
			hivePartitionManager.addPartition(partitionSpecification);
		} catch (HCatException e) {
			logger.alert(ALERT_TYPE.OTHER_ERROR, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.MAJOR,
					"\"hive partition creation failed \" database ={} tableName={} columnsSize = {} error={}", dbName,tableName,partitionMap, e.toString());
					throw e;			
		}	
	}
	/**
	 * 
	 * @param partitionKeys
	 * @param partitionValues
	 * @return
	 * @throws Exception
	 */
	private HashMap<String,String> getPartitionsMap(String partitionKeys,String partitionValues) {
		HashMap<String,String> partitionMap = new HashMap<String,String>();

		String[] partitionKeys01 = StringUtils.split(partitionKeys, ",");
		String[] partitionValues01 = StringUtils.split(partitionValues, ",");

		if(partitionKeys01.length != partitionValues01.length){
			throw new RuntimeException("partition keys and values need to be matched");
		}

		for (int i = 0 ; i < partitionKeys01.length ; i++) {
			partitionMap.put(partitionKeys01[i], partitionValues01[i]);
		}
		return partitionMap;
	}

	/**
	 * TODO : the type need derive from headers..JIRA opened.
	 * @param partitionKeys
	 * @return
	 */
	private List<Column> getPartitionsColumns(String partitionKeys){
		Column partitionColum = null;
		List<Column>  partitionColums = null;
		if(partitionKeys == null ){
			return partitionColums;
		}
		partitionColums = new ArrayList<Column>();
		String[] partitionKeys01 = StringUtils.split(partitionKeys, ",");
		for (int i = 0 ; i < partitionKeys01.length ; i++) {
			partitionColum = new Column(partitionKeys01[i],null,null);
			partitionColums.add(partitionColum);
		}
		return partitionColums;
	}
	
	public String getCompletePartitionPath(String hdfsBasePath,String hivePartitionValues){
		String hdfsCompletedPath = null;
		if (StringUtils.isNotBlank(hivePartitionValues)) {
			String[] partitionList = hivePartitionValues.split(",");
			StringBuilder stringBuilder = new StringBuilder();
			for (int i = 0; i < partitionList.length; i++) {
				stringBuilder.append(partitionList[i].trim() + File.separator);
			}
			String partitionPath = stringBuilder.toString();
			hdfsCompletedPath = hdfsBasePath + partitionPath;
		} else {
			hdfsCompletedPath = hdfsBasePath;
		}
		if (hdfsCompletedPath.endsWith(File.separator))
			hdfsCompletedPath = hdfsCompletedPath.substring(0, hdfsCompletedPath.length() - 1);
		
		hdfsCompletedPath = StringUtils.remove(hdfsCompletedPath, webhdfs_scheme);
		return hdfsCompletedPath;
	}

}

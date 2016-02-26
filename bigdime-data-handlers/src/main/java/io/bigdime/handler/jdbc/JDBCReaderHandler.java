package io.bigdime.handler.jdbc;


import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;

import javax.sql.DataSource;

@Component
@Scope("prototype")
/**
 * 
 * @author Murali Namburi, Pavan Sabinikari
 *
 */
public class JDBCReaderHandler extends AbstractHandler {

	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(JDBCReaderHandler.class));

	@Autowired
	private DataSource lazyConnectionDataSourceProxy;
	@Autowired
	private MetadataStore metadataStore;
	@Autowired
	RuntimeInfoStore<RuntimeInfo> runTimeInfoStore;
	@Autowired
	private JdbcInputDescriptor jdbcInputDescriptor;

	@Value("${database.driverClassName}")
	private String driverName;
	@Value("${split.size}")
	private String splitSize;
	@Value("${date.format}")
	private String DATE_FORMAT;
	//@Value("${runtime.entry.initial.date.value}")
	private static String initialRuntimeDateEntry="1900-01-01 00:00:00";

	private JdbcTemplate jdbcTemplate;

	private String sql;
	@Value("${hiveDBName}")
	private String hiveDBName;
	// private boolean splitByFlag = false;
	private String columnValue;
	private String highestIncrementalColumnValue;
	//private String maxIncrementalTableColumnValue;
	private Metasegment metasegment;

	private JdbcMetadataManagement jdbcMetadataManagment;
	//private JdbcRuntimeManagement jdbcRuntimeManagement;
	private String handlerPhase = null;
	private String jsonStr = null;

	//private int rowCount = 0;

	@Override
	public void build() throws AdaptorConfigurationException {
		handlerPhase = "building JDBC Reader Handler";
		super.build();
		logger.info(handlerPhase,
				"handler_id={} handler_name={} properties={}", getId(),
				getName(), getPropertyMap());

		@SuppressWarnings("unchecked")
		Entry<String, String> srcDescInputs = (Entry<String, String>) getPropertyMap()
				.get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC);
		if (srcDescInputs == null) {
			throw new InvalidValueConfigurationException(
					"src-desc can't be null");
		}
		logger.info(handlerPhase,
				"entity:fileNamePattern={} input_field_name={}",
				srcDescInputs.getKey(), srcDescInputs.getValue());
		jsonStr = srcDescInputs.getKey();
		// jdbcInputDescriptor = new JdbcInputDescriptor();
		try {

			jdbcInputDescriptor.parseDescriptor(jsonStr);
			sql = jdbcInputDescriptor.formatQuery(driverName);
		} catch (IllegalArgumentException ex) {
			throw new InvalidValueConfigurationException(
					"incorrect value specified in src-desc, value must be in json string format");
		} catch (JdbcHandlerException e) {
			// TODO Auto-generated catch block
			logger.alert(ALERT_TYPE.INGESTION_FAILED,
					ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,
					"\"sql formatter exception\" inputDescription={} error={}",
					jsonStr, e.toString());
			throw new AdaptorConfigurationException(e);
		}
	}

	@Override
	public Status process() throws HandlerException {
		logger.info(handlerPhase,
				"handler_id={} handler_name={} properties={}", getId(),
				getName(), getPropertyMap());
		Status adaptorThreadStatus = null;
		try {
			adaptorThreadStatus = preProcess();
		} catch (RuntimeInfoStoreException e) {
			//e.printStackTrace();
			logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,
					"\"jdbcAdaptor RuntimeInfoStore exception\"  TableName = {} error={}",
					jdbcInputDescriptor.getEntityName(), e.toString());
		} catch (JSONException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,"\"jdbcAdaptor json formatter exception\" jsonString={} error={}",
					jsonStr, e.toString());
		} catch (JdbcHandlerException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,"\"jdbcAdaptor jdbcHandler exception\" TableName={} error={}",
					jdbcInputDescriptor.getEntityName(), e.toString());
		}

		if (adaptorThreadStatus != null) {
			return adaptorThreadStatus;
		} else {
			return Status.READY;
		}
	}

	public void setDataSource(DataSource dataSource) {
		this.lazyConnectionDataSourceProxy = dataSource;
	}

	public JDBCReaderHandler() {
		super();

	}

	/**
	 * This method does all the pre-processing like update the column details to
	 * Metadata, Runtime Information maintenance.
	 * 
	 * @return
	 * @throws JdbcHandlerException
	 * @throws JSONException
	 * @throws RuntimeInfoStoreException
	 */
	public ActionEvent.Status preProcess() throws JdbcHandlerException,
			JSONException, RuntimeInfoStoreException {
		// Get configured table details..
		boolean processFlag = false;
		jdbcTemplate = new JdbcTemplate(lazyConnectionDataSourceProxy);
		jdbcMetadataManagment = new JdbcMetadataManagement();
		if (!StringUtils.isEmpty(sql)) {
			// format Query...

			logger.debug("Formatted JDBC Reader Handler Query", "sql={}", sql);
			// Get Source Metadata..
			metasegment = jdbcMetadataManagment.getSourceMetadata(
					jdbcInputDescriptor, jdbcTemplate);
			

			logger.debug("Retrieved JDBC Reader Handler Source Metadata",
					"Metasegment={}", metasegment);

			jdbcMetadataManagment.setColumnList(jdbcInputDescriptor,
					metasegment);

			logger.debug("JDBC Reader Handler source column list",
					"ColumnList={}", jdbcInputDescriptor.getColumnList());

			if (jdbcInputDescriptor.getColumnList().size() == JdbcConstants.INTEGER_CONSTANT_ZERO)
				throw new JdbcHandlerException(
						"Unable to retrieve the column list for the table Name = "
								+ jdbcInputDescriptor.getEntityName());
			// throw Exception:
			// if the incrementedBy column doesn't exist in the table..
			if (jdbcInputDescriptor.getIncrementedBy().length() > JdbcConstants.INTEGER_CONSTANT_ZERO
					&& jdbcInputDescriptor.getIncrementedColumnType() == null) {
				throw new JdbcHandlerException(
						"IncrementedBy Value doesn't exist in the table column list");
			}

			// Put into Metadata...
			jdbcMetadataManagment.checkAndUpdateMetadata(metasegment,
					jdbcInputDescriptor.getTargetEntityName(),
					jdbcInputDescriptor.getColumnList(), metadataStore,hiveDBName);
			// Check if Runtime details Exists..
			//maxIncrementalTableColumnValue=(String)jdbcTemplate.queryForObject(
				//	"SELECT MAX("+jdbcInputDescriptor.getIncrementedBy()+") FROM  "+jdbcInputDescriptor.getEntityName(),  String.class);
				if (getOneQueuedRuntimeInfo(runTimeInfoStore,jdbcInputDescriptor.getEntityName()) == null){

				// Insert into Runtime Data...
				if (jdbcInputDescriptor.getIncrementedBy() != null
						&& jdbcInputDescriptor.getIncrementedBy().length() > JdbcConstants.INTEGER_CONSTANT_ZERO) {
					
					HashMap<String, String> properties = new HashMap<String, String>();
					if (driverName
							.equalsIgnoreCase(JdbcConstants.ORACLE_DRIVER_NAME)
							&& jdbcInputDescriptor.getIncrementedColumnType()
									.equalsIgnoreCase("DATE"))
						properties.put(jdbcInputDescriptor.getIncrementedBy(),
								initialRuntimeDateEntry);
					else
						properties.put(jdbcInputDescriptor.getIncrementedBy(),
								JdbcConstants.INTEGER_CONSTANT_ZERO + "");
					//if (StringUtils.isNotEmpty(maxIncrementalTableColumnValue))
					//	properties.put(JdbcConstants.MAX_INCREMENTAL_COLUMN_VALUE, maxIncrementalTableColumnValue);
					boolean runtimeInsertionFlag = updateRuntimeInfo(
							runTimeInfoStore,
							jdbcInputDescriptor.getEntityName(),
							jdbcInputDescriptor.getIncrementedColumnType(),
							RuntimeInfoStore.Status.QUEUED, properties);

					logger.info(
							"JDBC Reader Handler inserting Runtime data",
							"tableName={} PropertyKey={} PropertyValue={} status={}",
							jdbcInputDescriptor.getEntityName(),
							jdbcInputDescriptor.getIncrementedBy(),
							columnValue, runtimeInsertionFlag);
				}
				processFlag = processRecords();
			} else {
				logger.debug(
						"JDBC Reader Handler processing an existing table ",
						"tableName={}", jdbcInputDescriptor.getEntityName());
				processFlag = processRecords();
			}

		}
		if (processFlag) {
			return Status.CALLBACK;
		} else
			return Status.BACKOFF;
	}
	

	private String getCurrentColumnValue() {
		String currentIncrementalColumnValue = null;
		RuntimeInfo runtimeInfo = null;
		try {
			runtimeInfo = getOneQueuedRuntimeInfo(runTimeInfoStore,jdbcInputDescriptor.getEntityName());
		} catch (RuntimeInfoStoreException e) {
			
			logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,
					"\"jdbcAdaptor RuntimeInfoStore exception while getting current value\" TableName:{} error={}",
					jdbcInputDescriptor.getEntityName(),e.toString());
		}
		if (runtimeInfo != null)
			currentIncrementalColumnValue = runtimeInfo.getProperties().get(
					jdbcInputDescriptor.getIncrementedBy());
		return currentIncrementalColumnValue;
	}

	/**
	 * This methods fetches the number of records from the source.
	 * 
	 * @return
	 */
	public boolean processRecords() {
		boolean splitByFlag = true;
		jdbcTemplate = new JdbcTemplate(lazyConnectionDataSourceProxy);
		
		String repoColumnValue = getCurrentColumnValue();
		logger.debug("JDBC Reader Handler in process records",
				"Latest Incremented Repository Value= {}", repoColumnValue);
		if (repoColumnValue != null)
			columnValue = repoColumnValue;
		logger.debug("JDBC Reader Handler in processing ",
				"actual sql={} latestIncrementalValue={}", sql, columnValue);
		List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
		jdbcTemplate.setQueryTimeout(120);
		long startTime = System.currentTimeMillis();
		if (sql.contains(JdbcConstants.QUERY_PARAMETER)) {
			if (columnValue != null) {
				
					if (driverName.indexOf(JdbcConstants.ORACLE_DRIVER) > JdbcConstants.INTEGER_CONSTANT_ZERO
							&& jdbcInputDescriptor.getIncrementedColumnType()
									.equalsIgnoreCase("DATE")) {
						
						rows = jdbcTemplate
								.queryForList(
										sql,
										new Object[] { Timestamp.valueOf(columnValue)});
						;
												
					} else
						rows = jdbcTemplate.queryForList(sql,
								new Object[] { columnValue });
				
			} else
				rows = jdbcTemplate.queryForList(sql,
						new Object[] { JdbcConstants.INTEGER_CONSTANT_ZERO });
		} else {
			rows = jdbcTemplate.queryForList(sql);
		}
		long endTime = System.currentTimeMillis();
		logger.info(
				"JDBC Reader Handler during processing records",
				"Time taken to fetch records from table ={} from a columnValue={} with a fetchSize={} is {} milliseconds",
				jdbcInputDescriptor.getEntityName(), columnValue, splitSize,
				(endTime - startTime));
		logger.debug("JDBC Reader Handler during processing records","columns in the tableName={} are {}",
				jdbcInputDescriptor.getEntityName(),jdbcInputDescriptor.getColumnList());
		// Process each row to HDFS...
		if (rows.size() > JdbcConstants.INTEGER_CONSTANT_ZERO)
			processEachRecord(rows);
		else {
			logger.info("JDBC Reader Handler during processing records",
					"No more rows found for query={}", sql);
			splitByFlag = false;
		}
		// Assigning the current incremental value..
		if (jdbcInputDescriptor.getIncrementedBy().length() > JdbcConstants.INTEGER_CONSTANT_ZERO
				&& highestIncrementalColumnValue != null) {
			
			HashMap<String, String> properties = new HashMap<String, String>();
			properties.put(jdbcInputDescriptor.getIncrementedBy(), highestIncrementalColumnValue);
			//if(StringUtils.isNotEmpty(maxIncrementalTableColumnValue))
			//	properties.put(JdbcConstants.MAX_INCREMENTAL_COLUMN_VALUE, maxIncrementalTableColumnValue);
			boolean highestValueStatus=false;
			try {
				highestValueStatus = updateRuntimeInfo(
						runTimeInfoStore,
						jdbcInputDescriptor.getEntityName(),
						jdbcInputDescriptor.getIncrementedColumnType(),
						RuntimeInfoStore.Status.QUEUED, properties);
			} catch (RuntimeInfoStoreException e) {
				
				logger.alert(ALERT_TYPE.INGESTION_FAILED,
						ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
						ALERT_SEVERITY.BLOCKER,
						"\"jdbcAdaptor RuntimeInfo Update exception\" TableName={} error={}",
						jdbcInputDescriptor.getEntityName(), e.toString());
			}
			logger.info("JDBC Reader Handler saved highest incremental value",
					"incremental column saved status(true/false)?",highestValueStatus);
			columnValue = highestIncrementalColumnValue;
		}
		if (jdbcInputDescriptor.getIncrementedBy().length() == JdbcConstants.INTEGER_CONSTANT_ZERO
				|| jdbcInputDescriptor.getIncrementedBy() == null)
			splitByFlag = false;

		return splitByFlag;
	}

	/**
	 * This method sets each record in Action Event for further Data cleansing
	 * 
	 * @param rows
	 */
	private void processEachRecord(List<Map<String, Object>> rows) {
        int rowCount=0;
		List<ActionEvent> actionEvents = new ArrayList<ActionEvent>();
		for (Map<String, Object> row : rows) {
            ++rowCount;
			// Preparing data to handover to DataCleansing Handler
			byte[] body = SerializationUtils.serialize((Serializable) row);
			if (body != null) {
				ActionEvent actionEvent = new ActionEvent();
				actionEvent.setBody(body);
				actionEvents.add(actionEvent);

				highestIncrementalColumnValue = row.get(jdbcInputDescriptor
						.getColumnList().get(
								jdbcInputDescriptor.getColumnList().indexOf(
										jdbcInputDescriptor.getColumnName())))
						+ "";
              if(rowCount==rows.size()) {
            	  List<ActionEvent> ignoredActionEventList = getIgnoredBatchRecords(sql.replaceAll(">","="),row,highestIncrementalColumnValue);
            	  if(ignoredActionEventList!=null && ignoredActionEventList.size() > 0){
            		  actionEvents.addAll(ignoredActionEventList);
            	  }
              
              }
			}
		}
		if (actionEvents.size() > 0){
			
			/*if (highestIncrementalColumnValue.equalsIgnoreCase(maxIncrementalTableColumnValue)){
				int lastEvent = actionEvents.size();
				ActionEvent actionEvent = actionEvents.get(lastEvent - 1);
				
				
				if (actionEvent!=null){
					actionEvent.getHeaders().put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.TRUE.toString());
				
				}
			}*/
			getHandlerContext().setEventList(actionEvents);
		}
	}
	
	public List<ActionEvent> getIgnoredBatchRecords(String ignoredRowsSql,Map<String,Object> lastActionEventRow, String conditionValue){
		List<ActionEvent> ignoredActionEvents = new ArrayList<ActionEvent>();
		
		
		boolean isRowExist = false;
		List<Map<String, Object>> ignoredRows = new ArrayList<Map<String, Object>>();
		if (ignoredRowsSql.contains(JdbcConstants.QUERY_PARAMETER)) {
			if (conditionValue != null) {
					if (driverName.indexOf(JdbcConstants.ORACLE_DRIVER) > JdbcConstants.INTEGER_CONSTANT_ZERO
							&& jdbcInputDescriptor.getIncrementedColumnType()
									.equalsIgnoreCase("DATE")) {
						
						ignoredRows = jdbcTemplate
								.queryForList(
										ignoredRowsSql,
										new Object[] { Timestamp.valueOf(conditionValue)});
						;
												
					} else
						ignoredRows = jdbcTemplate.queryForList(ignoredRowsSql,
								new Object[] { conditionValue });
			} else
				ignoredRows = jdbcTemplate.queryForList(ignoredRowsSql,
						new Object[] { JdbcConstants.INTEGER_CONSTANT_ZERO });
		} else {
			ignoredRows = jdbcTemplate.queryForList(ignoredRowsSql);
		}
		
		if (ignoredRows.size() > JdbcConstants.INTEGER_CONSTANT_ZERO)
			//processEachRecord(ignoredRows);
			for(Map<String,Object> ignoredRow: ignoredRows){
				boolean comparedRowsStatus = ignoredRow.equals(lastActionEventRow);
				if(comparedRowsStatus) {
					isRowExist = true;
					continue;
				} else {
					if(isRowExist) {
						byte[] body = SerializationUtils.serialize((Serializable) ignoredRow);
						if (body != null) {
							ActionEvent actionEvent = new ActionEvent();
							actionEvent.setBody(body);
							ignoredActionEvents.add(actionEvent);
						}
					}
				}
			}
		else {
			logger.info("JDBC Reader Handler during Ignored processing records",
					"No more rows found for query={}", ignoredRowsSql);
			//splitByFlag = false;
		}
		if(ignoredActionEvents.size() > 0) return ignoredActionEvents;
		return null;
	}

}


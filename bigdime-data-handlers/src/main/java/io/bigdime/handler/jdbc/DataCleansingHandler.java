/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.commons.lang.SerializationUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.core.handler.HandlerJournal;
import io.bigdime.core.handler.SimpleJournal;

/**
 * 
 * @author Murali Namburi, Pavan Sabinikari
 * 
 */
@Component
@Scope("prototype")
public class DataCleansingHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(DataCleansingHandler.class));

	@Autowired
	private JdbcInputDescriptor jdbcInputDescriptor;
	@Autowired
	private DataSource sqlDataSource;
	@Value("${database.driverClassName}")
	private String driverName;
	@Value("${date.format}")
	private String DATE_FORMAT;

	private JdbcTemplate jdbcTemplate;
	JdbcMetadataManagement jdbcMetadataManagment;
	private String handlerPhase = null;
	private String jsonStr = null;

	@Override
	public void build() throws AdaptorConfigurationException {
		handlerPhase = "build DataCleansingHandler";
		logger.info(handlerPhase,"handler_id={} handler_name={} properties={}", getId(),
							getName(), getPropertyMap());
		super.build();

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

		try {

			jdbcInputDescriptor.parseDescriptor(jsonStr);
		} catch (IllegalArgumentException ex) {
			throw new InvalidValueConfigurationException(
					"incorrect value specified in src-desc, value must be in json string format");
		}
	}

	public JdbcMetadataManagement getMetadataManagementInstance() {
		return new JdbcMetadataManagement();
	}

	public JdbcTemplate getJdbcTemplateInstace() {
		return new JdbcTemplate(sqlDataSource);
	}

	@Override
	public Status process() throws HandlerException {
		handlerPhase = "process DataCleansingHandler";

		logger.debug(handlerPhase,
				"handler_id={} handler_name={} properties={}", getId(),
				getName(), getPropertyMap());

		jdbcMetadataManagment = getMetadataManagementInstance();
		jdbcTemplate = getJdbcTemplateInstace();
		Metasegment metasegment = jdbcMetadataManagment.getSourceMetadata(
				jdbcInputDescriptor, jdbcTemplate);

		logger.debug("Retrieved Data Cleansing Handler Source Metadata",
				"Metasegment={}", metasegment);

		jdbcMetadataManagment.setColumnList(jdbcInputDescriptor, metasegment);
		if (getSimpleJournal().getEventList() != null
				&& !getSimpleJournal().getEventList().isEmpty()) {
			// process for CALLBACK status.
			/*
			 * @formatter:off Get the list from journal remove one from the list
			 * submit to channel if needed set one in context if more available
			 * in journal list, return CALLBACK
			 * 
			 * @formatter:on
			 */
			List<ActionEvent> actionEvents = getSimpleJournal().getEventList();
			logger.debug("process DataCleansingHandler",
					"_message=\"journal not empty\" list_size={}",
					actionEvents.size());
			return processIt(actionEvents);

		} else {
			// process for ready status.
			/*
			 * @formatter:off Get the list from context remove one from the list
			 * submit to channel if needed set one in context if more available
			 * in context list, return CALLBACK
			 * 
			 * @formatter:on
			 */
			List<ActionEvent> actionEvents = getHandlerContext().getEventList();
			logger.debug("process DataCleansingHandler",
					"_message=\"journal empty, will process from context\" actionEvents={}",actionEvents);

			Preconditions.checkNotNull(actionEvents);
			Preconditions
					.checkArgument(!actionEvents.isEmpty(),
							"eventList in HandlerContext must contain at least one ActionEvent");
			return processIt(actionEvents);
		}
	}

	/**
	 * Processes each record to Channel
	 * 
	 * @param actionEvents
	 * @return
	 * @throws HandlerException
	 */
	@SuppressWarnings("unchecked")
	private Status processIt(List<ActionEvent> actionEvents)
			throws HandlerException {
		Status statusToReturn = Status.READY;
		long startTime = System.currentTimeMillis();
		while (!actionEvents.isEmpty()) {
			ActionEvent actionEvent = actionEvents.remove(0);

			byte[] data = actionEvent.getBody();

			StringBuffer sbFormattedRowContent = new StringBuffer();
			StringBuffer sbHiveNonPartitionColumns = new StringBuffer();
			String datePartition = null;
			// @SuppressWarnings("unchecked")
			Map<String, Object> row = (Map<String, Object>) SerializationUtils
					.deserialize(data);

			if (row != null) {
				for (int columnNamesListCount = 0; columnNamesListCount < jdbcInputDescriptor
						.getColumnList().size(); columnNamesListCount++) {
					// Ensure each field doesn't have rowlineDelimeter
					//Pattern p = Pattern.compile(jdbcInputDescriptor.getRowDelimeter() -- "\n|\r");
					Pattern p = Pattern.compile(JdbcConstants.FIELD_CHARACTERS_TO_REPLACE);
					StringBuffer sbfMatcher = new StringBuffer();
					
					Matcher m = p.matcher(sbfMatcher.append(row.get(jdbcInputDescriptor.getColumnList().
										get(columnNamesListCount))));
					StringBuffer sbFormattedField = new StringBuffer();
					while (m.find()) {
						m.appendReplacement(sbFormattedField, JdbcConstants.FIELD_CHARACTERS_REPLACE_BY);
					}
					m.appendTail(sbFormattedField);
					sbFormattedRowContent.append(sbFormattedField);
					if (columnNamesListCount != jdbcInputDescriptor.getColumnList().size() - 1)
						sbFormattedRowContent.append(jdbcInputDescriptor.getFieldDelimeter());

					if (jdbcInputDescriptor.getIncrementedColumnType().indexOf("DATE") >= JdbcConstants.INTEGER_CONSTANT_ZERO
							    || jdbcInputDescriptor.getIncrementedColumnType().indexOf("TIMESTAMP") >= JdbcConstants.INTEGER_CONSTANT_ZERO) {

						if (jdbcInputDescriptor.getColumnList().get(columnNamesListCount)
								.equalsIgnoreCase(jdbcInputDescriptor.getIncrementedBy())) {
							
								datePartition = Timestamp.valueOf(row
										.get(jdbcInputDescriptor.getColumnList().get(columnNamesListCount))
										.toString()).toString().substring(0,10).replaceAll("-","");
						}
					}
				}

				//Each Row is delimited by "\n"
				
				sbFormattedRowContent.append(jdbcInputDescriptor.getRowDelimeter());
				actionEvent.setBody(sbFormattedRowContent.toString().getBytes());

				
				sbHiveNonPartitionColumns.append(ActionEventHeaderConstants.ENTITY_NAME);
				actionEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME.toUpperCase(),
						             jdbcInputDescriptor.getTargetEntityName());
				
				actionEvent.getHeaders().put(ActionEventHeaderConstants.LINES_TERMINATED_BY,
									 jdbcInputDescriptor.getRowDelimeter());
				
				actionEvent.getHeaders().put(ActionEventHeaderConstants.FIELDS_TERMINATED_BY,
									 jdbcInputDescriptor.getFieldDelimeter());

				// Partition Dates logic..
				if (jdbcInputDescriptor.getSnapshot() != null
									  && jdbcInputDescriptor.getSnapshot().equalsIgnoreCase("YES")) {
					// get current date and format it to string
					actionEvent.getHeaders().put(ActionEventHeaderConstants.DATE, 
										 dateFormatHolder.get().format(new Date()));
				} else {
				if (datePartition != null) {
					actionEvent.getHeaders().put(ActionEventHeaderConstants.DATE, datePartition);
				} else
					actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_REQUIRED, "false");
					
				}
				
				
				if (actionEvent.getHeaders().get(ActionEventHeaderConstants.ENTITY_NAME.toUpperCase()) != null)
					actionEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME,
										 actionEvent.getHeaders().get(ActionEventHeaderConstants.ENTITY_NAME.toUpperCase()));
				
				
				if (actionEvent.getHeaders().get(ActionEventHeaderConstants.DATE) != null){
					actionEvent.getHeaders().put(ActionEventHeaderConstants.INPUT_DESCRIPTOR, 
										 actionEvent.getHeaders().get(ActionEventHeaderConstants.DATE));
				} else
					actionEvent.getHeaders().put(ActionEventHeaderConstants.INPUT_DESCRIPTOR, 
										 actionEvent.getHeaders().get(ActionEventHeaderConstants.ENTITY_NAME) + JdbcConstants.WITH_NO_PARTITION);
				    
				actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_NON_PARTITION_NAMES, 
						    		 sbHiveNonPartitionColumns.toString());
				if(actionEvents.isEmpty())
					actionEvent.getHeaders().put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.TRUE.toString());
				/*
				 * Check for outputChannel map. get the eventList of channels.
				 * check the criteria and put the message.
				 */
				if (getOutputChannel() != null) {
					getOutputChannel().put(actionEvent);
				}

			}
		}
		long endTime = System.currentTimeMillis();

		logger.info("Data Cleansing Handler during processing records","Time taken to process action Events is ={} milliseconds",
										(endTime - startTime));
		// getHandlerContext().createSingleItemEventList(actionEvent);
		if (!actionEvents.isEmpty()) {
			getSimpleJournal().setEventList(actionEvents);
			statusToReturn = Status.CALLBACK;
		} else {
			getSimpleJournal().setEventList(null);
			statusToReturn = Status.READY;
		}

		return statusToReturn;

	}
	
	
	/**
	 * dateFormatHolder.get() used to get an instance of SimpleDateFormat object
	 */
	private static final ThreadLocal<SimpleDateFormat> dateFormatHolder = new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyyMMdd");
		}
	};

	private HandlerJournal getSimpleJournal() throws HandlerException {
		HandlerJournal simpleJournal = getNonNullJournal(SimpleJournal.class);
		return simpleJournal;
	}

}

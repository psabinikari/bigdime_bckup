package io.bigdime.handler.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

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
			LoggerFactory.getLogger(JDBCReaderHandler2.class));

	@Autowired
	private DataSource sqlDataSource;
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
	@Value("${runtime.entry.initial.date.value}")
	private String initialRuntimeDateEntry;

	private JdbcTemplate jdbcTemplate;

	private String sql;
	// private boolean splitByFlag = false;
	private String columnValue;
	private String highestIncrementalColumnValue;
	private Metasegment metasegment;

	private JdbcMetadataManagement jdbcMetadataManagment;
	private JdbcRuntimeManagement jdbcRuntimeManagement;
	private String handlerPhase = null;
	private String jsonStr = null;

	private int rowCount = 0;

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
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (JdbcHandlerException e) {
			e.printStackTrace();
		}

		if (adaptorThreadStatus != null) {
			return adaptorThreadStatus;
		} else {
			return Status.READY;
		}
	}

	public void setDataSource(DataSource dataSource) {
		this.sqlDataSource = dataSource;
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
	 */
	public ActionEvent.Status preProcess() throws JdbcHandlerException,
			JSONException {
		// Get configured table details..
		boolean processFlag = false;
		jdbcTemplate = new JdbcTemplate(sqlDataSource);
		jdbcMetadataManagment = new JdbcMetadataManagement();
		jdbcRuntimeManagement = new JdbcRuntimeManagement();
		if (!StringUtils.isEmpty(sql)) {
			// format Query...

			logger.debug("Formatted JDBC Reader Handler Query", "sql={}", sql);
			// Get Source Metadata..
			metasegment = jdbcMetadataManagment.getSourceMetadata(jdbcInputDescriptor, jdbcTemplate);
			
			logger.debug("Retrieved JDBC Reader Handler Source Metadata","Metasegment={}", metasegment);
			
			jdbcMetadataManagment.setColumnList(jdbcInputDescriptor,metasegment);
			
			logger.debug("JDBC Reader Handler source column list","ColumnList={}", jdbcInputDescriptor.getColumnList());
			
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
			jdbcMetadataManagment.checkAndUpdateMetadata(metasegment,jdbcInputDescriptor.getEntityName(),
											jdbcInputDescriptor.getColumnList(), metadataStore);
			// Check if Runtime details Exists..
			if (jdbcRuntimeManagement.findRTIEntryExistence(jdbcInputDescriptor.getEntityName(),
					jdbcInputDescriptor.getIncrementedColumnType(),runTimeInfoStore) == JdbcConstants.INTEGER_CONSTANT_ZERO) {

				// Insert into Runtime Data...
				if (jdbcInputDescriptor.getIncrementedBy() != null
						&& jdbcInputDescriptor.getIncrementedBy().length() > JdbcConstants.INTEGER_CONSTANT_ZERO) {
					boolean runtimeInsertionFlag = jdbcRuntimeManagement
							.insertRuntimeEntry(jdbcInputDescriptor
									.getIncrementedColumnType(), driverName,
									jdbcInputDescriptor,
									initialRuntimeDateEntry, runTimeInfoStore);

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
			return Status.READY;
	}

	/**
	 * This methods fetches the number of records from the source.
	 * 
	 * @return
	 */
	public boolean processRecords() {
		boolean splitByFlag = true;
		jdbcTemplate = new JdbcTemplate(sqlDataSource);
		String repoColumnValue = jdbcRuntimeManagement
				.findColumnValueByTableNameFromRTI(jdbcInputDescriptor,
						jdbcInputDescriptor.getIncrementedColumnType(),
						runTimeInfoStore);
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
				try {
					if (driverName.indexOf(JdbcConstants.ORACLE_DRIVER) > JdbcConstants.INTEGER_CONSTANT_ZERO
							&& jdbcInputDescriptor.getIncrementedColumnType()
									.equalsIgnoreCase("DATE")) {
						rows = jdbcTemplate
								.queryForList(
										sql,
										new Object[] { new Timestamp(
												(new SimpleDateFormat(
														DATE_FORMAT)).parse(
														columnValue).getTime()) });
					} else
						rows = jdbcTemplate.queryForList(sql,
								new Object[] { columnValue });
				} catch (DataAccessException | ParseException e) {
					logger.alert(
							ALERT_TYPE.INGESTION_FAILED,
							ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
							ALERT_SEVERITY.BLOCKER,
							"\"Incremental column: Date parser exception\" inputDescription={} error={}",
							columnValue, e.toString());
				}
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
		logger.debug("JDBC Reader Handler during processing records",
				"columns in the tableName={} are {}",
				jdbcInputDescriptor.getEntityName(),
				jdbcInputDescriptor.getColumnList());
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
			boolean highestValueStatus = jdbcRuntimeManagement
					.saveCurrentHighestColumnValue(
							jdbcInputDescriptor.getIncrementedColumnType(),
							jdbcInputDescriptor, highestIncrementalColumnValue,
							runTimeInfoStore);

			logger.info("JDBC Reader Handler saved highest incremental value",
					"incremental column saved status(true/false)?",
					highestValueStatus);
			columnValue = highestIncrementalColumnValue;
		}
		if (jdbcInputDescriptor.getIncrementedBy().length() == JdbcConstants.INTEGER_CONSTANT_ZERO
				|| jdbcInputDescriptor.getIncrementedBy() == null)
			splitByFlag = false;

		return splitByFlag;
	}

	private byte[] convertToBytes(Object object) throws IOException {
		byte[] bytes = null;
		ByteArrayOutputStream bos = null;
		ObjectOutputStream oos = null;
		try {
			bos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(bos);
			oos.writeObject(object);
			oos.flush();
			bytes = bos.toByteArray();
		} finally {
			if (oos != null) {
				oos.close();
			}
			if (bos != null) {
				bos.close();
			}
		}
		return bytes;
	}

	/**
	 * This method sets each record in Action Event for further Data cleansing
	 * 
	 * @param rows
	 */
	private void processEachRecord(List<Map<String, Object>> rows) {

		List<ActionEvent> actionEvents = new ArrayList<ActionEvent>();
		for (Map<String, Object> row : rows) {

			// write the data to HDFS...
			// byte[] body = SerializationUtils.serialize((Serializable) row);
			byte[] body = null;
			try {
				body = convertToBytes(row);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (body != null) {
				ActionEvent actionEvent = new ActionEvent();
				actionEvent.setBody(body);
				System.out.println("byte count in JDBCReaderHandler"
						+ body.length);
				actionEvents.add(actionEvent);

				highestIncrementalColumnValue = row.get(jdbcInputDescriptor.getColumnList()
								.get(jdbcInputDescriptor.getColumnList().indexOf(jdbcInputDescriptor.getColumnName())))+ "";

			}

		}
		if (actionEvents.size() > 0)
			getHandlerContext().setEventList(actionEvents);
	}

}

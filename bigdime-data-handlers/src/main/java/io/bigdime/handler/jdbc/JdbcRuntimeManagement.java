package io.bigdime.handler.jdbc;

import java.util.HashMap;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
/**
 * 
 * @author Pavan Sabinikari
 *
 */
@Component
@Scope("prototype")
public class JdbcRuntimeManagement {

	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(JdbcRuntimeManagement.class));
	
	/**
	 * 
	 * @param tableName
	 * @return
	 */
	public int findRTIEntryExistence(String tableName, String incrementedColumnType,RuntimeInfoStore<RuntimeInfo> runTimeInfoStore) {
		// Find the existence in the Runtime...
		RuntimeInfo runtimeInfo = null;
		try {
			logger.debug("JDBC Reader Handler checking runtime entry existence","chekcing for TableName={} with column type={}", 
							tableName,incrementedColumnType);
			if (incrementedColumnType != null && incrementedColumnType.length() > JdbcConstants.INTEGER_CONSTANT_ZERO) {
				runtimeInfo = runTimeInfoStore.get("SQL_ADAPTOR", tableName,incrementedColumnType);
			}
		} catch (RuntimeInfoStoreException e) {

			logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
						 ALERT_SEVERITY.BLOCKER,"\"runtimeInfo retrieval error\" inputDescription={} error={}",
						 tableName, e.toString());
		}
		logger.debug("JDBC Reader Handler after checking runtime entry existence","Runtime info value = {}", runtimeInfo);
		if (runtimeInfo != null)
			return JdbcConstants.INTEGER_CONSTANT_NONZERO;
		else
			return JdbcConstants.INTEGER_CONSTANT_ZERO;

	}
	
	/**
	 * 
	 * @return
	 */
	public boolean insertRuntimeEntry(String incrementedColumnType,String driverName,JdbcInputDescriptor jdbcInputDescriptor, 
								String initialRuntimeDateEntry, RuntimeInfoStore<RuntimeInfo> runTimeInfoStore) {
		// Insert into RuntimeInfo
		boolean insertFlag = false;
		if (incrementedColumnType != null) {
			RuntimeInfo runtimeInfo = new RuntimeInfo();
			runtimeInfo.setAdaptorName("SQL_ADAPTOR");
			runtimeInfo.setEntityName(jdbcInputDescriptor.getEntityName());
			runtimeInfo.setInputDescriptor(incrementedColumnType);
			runtimeInfo.setNumOfAttempts(Integer.toString(1));
			runtimeInfo.setStatus(null);
			HashMap<String, String> properties = new HashMap<String, String>();
			if (driverName.equalsIgnoreCase("oracle.jdbc.driver.OracleDriver")
					&& incrementedColumnType.equalsIgnoreCase("DATE"))
				properties.put(jdbcInputDescriptor.getIncrementedBy(), initialRuntimeDateEntry);
			else
				properties.put(jdbcInputDescriptor.getIncrementedBy(), JdbcConstants.INTEGER_CONSTANT_ZERO + "");
			
			runtimeInfo.setProperties(properties);

			try {
				insertFlag = runTimeInfoStore.put(runtimeInfo);
			} catch (RuntimeInfoStoreException e) {

				logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
						ALERT_SEVERITY.BLOCKER,"\"runtimeInfo insertion error\" inputDescription={} error={}",
						jdbcInputDescriptor.getEntityName(), e.toString());
			}
		}
		logger.debug("JDBC Reader Handler inserting values in Runtime Information Managment(RTIM)",
				"inserting values for tableName={} with propertyKey={} . Insertion Staus (true/false)? ={}",
				jdbcInputDescriptor.getEntityName(), jdbcInputDescriptor.getIncrementedBy(),  insertFlag);

		return insertFlag;

	}
	
	/**
	 * 
	 * @param tableName
	 * @return
	 */
	public String findColumnValueByTableNameFromRTI(JdbcInputDescriptor jdbcInputDescriptor,String incrementedColumnType, 
								RuntimeInfoStore<RuntimeInfo> runTimeInfoStore) {
		// Find the property value from Runtime...
		String repoColumnValue = null;
		RuntimeInfo runtimeInfo = null;
		logger.debug("JDBC Reader Handler before getting current column entry value from runtime",
					 "TableName={} inputDescriptor={}", jdbcInputDescriptor.getEntityName(),
					 incrementedColumnType);
		try {
			if (incrementedColumnType != null)
				runtimeInfo = runTimeInfoStore.get("SQL_ADAPTOR", jdbcInputDescriptor.getEntityName(),
									incrementedColumnType);
		} catch (RuntimeInfoStoreException e) {

			logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,"\"runtimeInfo retrieval error\" inputDescription={} error={}",
					jdbcInputDescriptor.getEntityName(), e.toString());
		}
		logger.debug("JDBC Reader Handler after getting current column entry value from runtime",
				"RuntimeInfo value = {}", runtimeInfo);

		if (runtimeInfo != null)
			repoColumnValue = runtimeInfo.getProperties().get(jdbcInputDescriptor.getIncrementedBy());
		return repoColumnValue;

	}
	
	
	/**
	 * 
	 * @param highestColumnValue
	 * @return
	 */
	public boolean saveCurrentHighestColumnValue(String incrementedColumnType, JdbcInputDescriptor jdbcInputDescriptor,String highestColumnValue, RuntimeInfoStore<RuntimeInfo> runTimeInfoStore) {
		// Update to RuntimeInfo
		boolean highestColumnValueFlag = false;
		RuntimeInfo runtimeInfo = null;
		if (incrementedColumnType != null) {
			runtimeInfo = new RuntimeInfo();
			runtimeInfo.setAdaptorName("SQL_ADAPTOR");
			runtimeInfo.setEntityName(jdbcInputDescriptor.getEntityName());
			runtimeInfo.setInputDescriptor(incrementedColumnType);
			runtimeInfo.setNumOfAttempts(Integer.toString(1));
			runtimeInfo.setStatus(null);
			HashMap<String, String> properties = new HashMap<String, String>();
			if (highestColumnValue.length() > JdbcConstants.INTEGER_CONSTANT_ZERO)
				properties.put(jdbcInputDescriptor.getIncrementedBy(), highestColumnValue);
			runtimeInfo.setProperties(properties);

			try {
				highestColumnValueFlag = runTimeInfoStore.put(runtimeInfo);
			} catch (RuntimeInfoStoreException e) {
				logger.alert(
						ALERT_TYPE.INGESTION_FAILED,
						ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
						ALERT_SEVERITY.BLOCKER,
						"\"runtimeInfo saving highest current incremental Value error\" inputDescription={} error={}",
						jdbcInputDescriptor.getEntityName(), e.toString());
			}
		}

		logger.debug("JDBC Reader Handler saving values in Runtime Information Managment(RTIM)",
					 "Latest incremental column Value={} saved (true/false)? ={}",
					  highestColumnValue, highestColumnValueFlag);
		return highestColumnValueFlag;

	}



}

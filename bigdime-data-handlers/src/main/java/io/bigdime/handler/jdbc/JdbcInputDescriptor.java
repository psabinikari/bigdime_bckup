package io.bigdime.handler.jdbc;

import java.util.ArrayList;
import java.util.List;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.InputDescriptor;
import io.bigdime.core.commons.AdaptorLogger;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class JdbcInputDescriptor implements InputDescriptor<String>{

	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(JdbcInputDescriptor.class));

	@Value("${split.size}") private String splitSize;
	
	private String query;
	private String entityName;
	private String incrementedBy;
	private String incrementedColumnType;
	private String columnName;
	private String partition;
	private String fieldDelimeter;
	private String rowDelimeter;
	private String snapshot;
	
	private List<String> columnList = new ArrayList<String>();
	
	public List<String> getColumnList() {
		return columnList;
	}

	public void setColumnList(List<String> columnList) {
		this.columnList = columnList;
	}

	@Override
	public String getNext(List<String> availableInputDescriptors,
			String lastInputDescriptor) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void parseDescriptor(String jsonStr)  {
		// TODO Auto-generated method stub
		
		 
		if (jsonStr == null || jsonStr.length() <= 0)
			throw new IllegalArgumentException("descriptor can't be null or empty");
		try {
			JSONObject jsonObject = new JSONObject(jsonStr);
		
		query = jsonObject.getString("query");
		entityName = query.split(" ")[3];
		incrementedBy = jsonObject.getString("incrementedBy");
		partition = jsonObject.getString("partitionedColumns");
		if (!jsonObject.isNull("fieldDelimeter"))
			fieldDelimeter = jsonObject.getString("rowDelimeter");
		if (fieldDelimeter == null || fieldDelimeter.length() == 0)
			fieldDelimeter = JdbcConstants.CONTROL_A_DELIMETER;
		if (!jsonObject.isNull("rowDelimeter"))
			rowDelimeter = jsonObject.getString("rowDelimeter");
		if (rowDelimeter == null || rowDelimeter.length() == 0)
			rowDelimeter = JdbcConstants.NEW_LINE_DELIMETER;
		
		if (!jsonObject.isNull("snapshot"))
			snapshot = jsonObject.getString("snapshot");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		logger.debug("Retrieved JDBC Reader Handler Configurations","query={} tableName={} incrementedBy={} partitionedColumns={} fieldDelimeter={} rowDelimeter={}",
				query, entityName, incrementedBy, partition, fieldDelimeter, rowDelimeter);

		
	}
	
	public String formatQuery(String driverName) throws JdbcHandlerException {

		if (query.indexOf(JdbcConstants.QUERY_PARAMETER) < JdbcConstants.INTEGER_CONSTANT_ZERO && incrementedBy.length() > JdbcConstants.INTEGER_CONSTANT_ZERO) {
			query = query + " WHERE " + incrementedBy + " > " + JdbcConstants.QUERY_PARAMETER;
		}
		// throw Exception:
		// if incrementedBy and where clause parameters are different..
		if (query.indexOf(JdbcConstants.QUERY_PARAMETER) > JdbcConstants.INTEGER_CONSTANT_ZERO && incrementedBy.length() > JdbcConstants.INTEGER_CONSTANT_ZERO
				&& query.indexOf(incrementedBy) < JdbcConstants.INTEGER_CONSTANT_ZERO) {
			throw new JdbcHandlerException("incrementedBy value cannot be different from where condition");
		}

		if (query.indexOf(JdbcConstants.QUERY_PARAMETER) > JdbcConstants.INTEGER_CONSTANT_ZERO && incrementedBy.length() > JdbcConstants.INTEGER_CONSTANT_ZERO && query.indexOf(JdbcConstants.ORDER_BY_CLAUSE) < 0) {
			query = query + " ORDER BY " + incrementedBy + " ASC";
		}

		if(driverName.indexOf(JdbcConstants.ORACLE_DRIVER) > JdbcConstants.INTEGER_CONSTANT_ZERO && Integer.parseInt(splitSize) > JdbcConstants.INTEGER_CONSTANT_ZERO) {
			query = "SELECT t.* FROM ("+query+") t WHERE ROWNUM <"+splitSize;
		}

		return query;

	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public String getIncrementedBy() {
		return incrementedBy;
	}

	public void setIncrementedBy(String incrementedBy) {
		this.incrementedBy = incrementedBy;
	}
	
	public String getIncrementedColumnType() {
		return incrementedColumnType;
	}

	public void setIncrementedColumnType(String incrementedColumnType) {
		this.incrementedColumnType = incrementedColumnType;
	}
	
	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}


	public String getPartition() {
		return partition;
	}

	public void setPartition(String partition) {
		this.partition = partition;
	}

	public String getFieldDelimeter() {
		return fieldDelimeter;
	}

	public void setFieldDelimeter(String fieldDelimeter) {
		this.fieldDelimeter = fieldDelimeter;
	}

	public String getRowDelimeter() {
		return rowDelimeter;
	}

	public void setRowDelimeter(String rowDelimeter) {
		this.rowDelimeter = rowDelimeter;
	}

	public String getSnapshot() {
		return snapshot;
	}

	public void setSnapshot(String snapshot) {
		this.snapshot = snapshot;
	}

	

}

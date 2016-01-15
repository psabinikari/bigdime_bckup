package io.bigdime.handler.jdbc;

import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.commons.AdaptorLogger;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 * JdbcMetadata retrieves the metadata of fetched result set.
 * 
 * @author Pavan Sabinikari
 * 
 */

public class JdbcMetadata implements ResultSetExtractor<Metasegment> {

	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(JdbcMetadata.class));

	private Metasegment metasegment;
	private String tableName;

	public Metasegment extractData(ResultSet rs) throws SQLException,
			DataAccessException {
		if (rs != null) {
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			Set<Attribute> AttributesSet = new LinkedHashSet<Attribute>();
			for (int i = 1; i <= columnCount; i++) {

				Attribute attribute = new Attribute();
				attribute.setAttributeName(rsmd.getColumnName(i));
				attribute.setAttributeType(rsmd.getColumnTypeName(i));
				attribute.setIntPart(rsmd.getColumnDisplaySize(i) + "");
				attribute.setFractionalPart("");
				attribute.setNullable("NO");
				attribute.setComment("Not Null");
				attribute.setFieldType("COLUMN");
				AttributesSet.add(attribute);

				logger.debug("JDBC Reader Handler getting source Metadata ",
						"table Name: {} columnName:{} columnType:{} size:{}",rsmd.getTableName(i),
						rsmd.getColumnName(i), rsmd.getColumnType(i),
						rsmd.getColumnDisplaySize(i));
				tableName = rsmd.getTableName(i);
				
			}
 
			Set<Entitee> entitySet = new HashSet<Entitee>();
			Entitee entities = new Entitee();
			entities.setEntityName(tableName);
			entities.setEntityLocation("/data/sql/raw/tableName");
			entities.setDescription("HDFS LOCATION");
			entities.setVersion(1.0);
			entities.setAttributes(AttributesSet);
			entitySet.add(entities);

			metasegment = new Metasegment();
			metasegment.setAdaptorName("SQL_ADAPTOR");
			metasegment.setSchemaType("SQL");
			metasegment.setEntitees(entitySet);
			metasegment.setDatabaseName("MYSQL");
			metasegment.setDatabaseLocation("");
			metasegment.setRepositoryType("TARGET");
			metasegment.setIsDataSource("Y");

			metasegment.setCreatedBy("SQL_ADAPTOR");
			metasegment.setUpdatedBy("SQL_ADAPTOR");

		}
		return metasegment;
	}

}
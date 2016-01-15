package io.bigdime.handler.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.commons.AdaptorLogger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;


/**
 * 
 * @author Pavan Sabinikari
 *
 */

@Component
@Scope("prototype")
public class JdbcMetadataManagement {

	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(JdbcMetadataManagement.class));

	/**
	 * 
	 * @param jdbcInputDescriptor
	 * @param jdbcTemplate
	 * @return
	 */
	public Metasegment getSourceMetadata(JdbcInputDescriptor jdbcInputDescriptor, JdbcTemplate jdbcTemplate) {
		// jdbcTemplate.setFetchSize(100);
		// jdbcTemplate.setMaxRows(100);
		Metasegment metaSegmnt = (Metasegment) jdbcTemplate.query("SELECT * FROM " + jdbcInputDescriptor.getEntityName(),
																new JdbcMetadata());
		return metaSegmnt;
	}

	/**
	 * 
	 * @param metasegment
	 * @return
	 */
	public HashMap<String, String> getColumnList(
			JdbcInputDescriptor jdbcInputDescriptor, Metasegment metasegment) {
		// List<String> columnNames = new ArrayList<String>();
		HashMap<String, String> columnNamesAndTypes = new HashMap<String, String>();
		if (metasegment != null) {

			logger.debug("JDBC Handler Reader getting column list","tableName={}", jdbcInputDescriptor.getEntityName());
			Set<Entitee> entitySet = metasegment.getEntitees();
			for (Entitee entity : entitySet) {
				if (entity.getAttributes() != null)
					for (Attribute attribute : entity.getAttributes()) {
						columnNamesAndTypes.put(attribute.getAttributeName(),
								attribute.getAttributeType());
					}
			}
		} else
			throw new IllegalArgumentException("Provided argument:metasegment object in getColumnList() cannot be null");
		return columnNamesAndTypes;
	}

	/**
	 * 
	 * @param jdbcInputDescriptor
	 * @param metasegment
	 */
	public void setColumnList(JdbcInputDescriptor jdbcInputDescriptor,Metasegment metasegment) {
		List<String> columnNames = new ArrayList<String>();
		if (metasegment != null) {
			logger.debug("JDBC Handler Reader getting column list",
					"tableName={}", jdbcInputDescriptor.getEntityName());
			Set<Entitee> entitySet = metasegment.getEntitees();
			for (Entitee entity : entitySet) {
				if (entity.getAttributes() != null)
					for (Attribute attribute : entity.getAttributes()) {
						if (!columnNames.contains(attribute.getAttributeName()))
							columnNames.add(attribute.getAttributeName());

						if (jdbcInputDescriptor.getIncrementedBy().length() > JdbcConstants.INTEGER_CONSTANT_ZERO
								&& attribute.getAttributeName().equalsIgnoreCase(jdbcInputDescriptor.getIncrementedBy())) {
						
							jdbcInputDescriptor.setIncrementedColumnType(attribute.getAttributeType());
							// columnName = attribute.getAttributeName();
							jdbcInputDescriptor.setColumnName(attribute.getAttributeName());
						}
						if (jdbcInputDescriptor.getColumnName() == null) {
							jdbcInputDescriptor.setColumnName(attribute
									.getAttributeName());
						}
					}
			}

			if (jdbcInputDescriptor.getColumnList().size() > 0) jdbcInputDescriptor.getColumnList().clear();
			jdbcInputDescriptor.setColumnList(columnNames);

		} else
			throw new IllegalArgumentException(
					"Provided argument:metasegment object in getColumnList() cannot be null");
	}

	public void checkAndUpdateMetadata(Metasegment metasegment,String tableName, List<String> columnNamesList,
										MetadataStore metadataStore) {
		try {
			if (metasegment != null) {
				if (metasegment.getEntitees() != null)
					for (Entitee entity : metasegment.getEntitees()) {
						if (entity.getEntityName() == null || entity.getEntityName().length() == 0)
							entity.setEntityName(tableName);
					}
			}
			Metasegment metaseg = metadataStore.getAdaptorMetasegment("SQL_ADAPTOR", "SQL", tableName);
			if (metaseg != null && metaseg.getEntitees() != null) {
				for (Entitee entity : metaseg.getEntitees())
					if (entity == null || (entity.getAttributes().size() != columnNamesList.size()))
						metadataStore.put(metasegment);
			} else
				metadataStore.put(metasegment);
		} catch (MetadataAccessException e) {
			e.printStackTrace();
		}

	}

}

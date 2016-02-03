package io.bigdime.handler.jdbc;


import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockito.Matchers.anyString;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


import static org.mockito.Mockito.*;


public class JDBCMetadataManagmentTest {
	
	JdbcMetadataManagement jdbcMetadataManagement;
	
	@Mock
	JdbcTemplate jdbcTemplate;
	
	@Mock
	JdbcInputDescriptor jdbcInputDescriptor;
	
	@Mock
	Metasegment metasegment;
	
	@BeforeClass
	public void init(){
		initMocks(this);
		jdbcMetadataManagement = new JdbcMetadataManagement();
	}
	
	
	
	@Test
	public void testGetSourceMetadataNotNull(){
		when(jdbcTemplate.query(anyString(), Mockito.any(JdbcMetadata.class))).thenReturn(metasegment);
		Assert.assertNotNull(jdbcMetadataManagement.getSourceMetadata(jdbcInputDescriptor, jdbcTemplate));
	}
	
	@Test
	public void testCheckSourceMetadataNull(){
		Assert.assertNull(jdbcMetadataManagement.getSourceMetadata(jdbcInputDescriptor, jdbcTemplate));
	}
	
	@Test(expectedExceptions=IllegalArgumentException.class)
	public void testGetColumnListException() {
		jdbcMetadataManagement.getColumnList(jdbcInputDescriptor, null);
	}
	
	@Test
	public void testGetColumnList() {
		Attribute attribute = new Attribute();
		attribute.setAttributeName("testAttributeName");
		attribute.setAttributeType("testAttributeType");
		
		@SuppressWarnings("unchecked")
		Set<Attribute> attributeSet = (Set<Attribute>) Mockito.mock(Set.class);
		attributeSet.add(attribute);
		Entitee entity = new Entitee();
		entity.setEntityName("testEntityname");
		entity.setDescription("testDescription");
		entity.setVersion(1.0);
		entity.setAttributes(attributeSet);
		@SuppressWarnings("unchecked")
		Set<Entitee> entitySet = (Set<Entitee>) Mockito.mock(Set.class);
		entitySet.add(entity);
		@SuppressWarnings("unchecked")
		Iterator<Entitee> entityIterator = Mockito.mock(Iterator.class);
		when(metasegment.getEntitees()).thenReturn(entitySet);
		when(entitySet.iterator()).thenReturn(entityIterator);
		when(entityIterator.next()).thenReturn(entity);
		Assert.assertNotNull(jdbcMetadataManagement.getColumnList(jdbcInputDescriptor, metasegment));
	}
	
	@Test
	public void testCheckAndUpdateMetadata() {
		MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
		@SuppressWarnings("unchecked")
		List<String> columnNamesList = Mockito.mock(List.class);
		columnNamesList.add("testColumnName1");
		jdbcMetadataManagement.checkAndUpdateMetadata(metasegment, "testTableName", columnNamesList, metadataStore,"testdbName");
	}
	
	@Test
	public void testSetNullColumnList() {
		jdbcMetadataManagement.setColumnList(jdbcInputDescriptor, metasegment);
	}
	
	@Test
	public void testSetColumnList() {
		
		Attribute attribute = new Attribute();
		attribute.setAttributeName("testAttributeName");
		attribute.setAttributeType("testAttributeType");
		
		@SuppressWarnings("unchecked")
		Set<Attribute> attributeSet = (Set<Attribute>) Mockito.mock(Set.class);
		attributeSet.add(attribute);
		Entitee entity = new Entitee();
		entity.setEntityName("testEntityname");
		entity.setDescription("testDescription");
		entity.setVersion(1.0);
		entity.setAttributes(attributeSet);
		@SuppressWarnings("unchecked")
		Set<Entitee> entitySet = (Set<Entitee>) Mockito.mock(Set.class);
		entitySet.add(entity);
		@SuppressWarnings("unchecked")
		Iterator<Entitee> entityIterator = Mockito.mock(Iterator.class);
		when(metasegment.getEntitees()).thenReturn(entitySet);
		when(entitySet.iterator()).thenReturn(entityIterator);
		when(entityIterator.hasNext()).thenReturn(true,false);
		when(entityIterator.next()).thenReturn(entity);
		
		@SuppressWarnings("unchecked")
		Iterator<Attribute> attributeIterator = Mockito.mock(Iterator.class);
		when(Mockito.mock(Entitee.class).getAttributes()).thenReturn(attributeSet);
		when(attributeSet.iterator()).thenReturn(attributeIterator);
		when(attributeIterator.hasNext()).thenReturn(true,false);
		when(attributeIterator.next()).thenReturn(attribute);
		///Set<Attribute> attributeSet = Mockito.mock(Set.class);
		//when(jdbcInputDescriptor.getIncrementedBy().length()).thenReturn(8);
		when(jdbcInputDescriptor.getIncrementedBy()).thenReturn("testAttributeName");
		jdbcMetadataManagement.setColumnList(jdbcInputDescriptor, metasegment);
	}

}

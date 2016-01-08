/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.source;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.Source;
import io.bigdime.core.commons.StringHelper;
import io.bigdime.core.config.HandlerConfig;
import io.bigdime.core.config.SourceConfig;
import io.bigdime.core.handler.DummyConcreteHandler;
import io.bigdime.core.handler.HandlerFactory;
import io.bigdime.core.handler.HandlerFactoryTest;

@Configuration
@ContextConfiguration(classes = { DataSourceFactory.class, HandlerFactory.class, DummyConcreteHandler.class,
		StringHelper.class })

public class DataSourceFactoryTest extends AbstractTestNGSpringContextTests {
	@Autowired
	private DataSourceFactory dataSourceFactory;

	/**
	 * Assert that DataSourceFactory can be autowired.
	 * 
	 */
	@Test
	public void testGetInstance() {
		Assert.assertNotNull(dataSourceFactory);
	}

	public DataSourceFactoryTest() throws IOException {
		HandlerFactoryTest.initHandlerFactory();
	}

	/**
	 * Asseer that getDataSource throws an AdaptorConfigurationException if it's
	 * called with a null argument.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testGetDataSourceWithNullConfig() throws AdaptorConfigurationException {
		dataSourceFactory.getDataSource(null);
	}

	/*
	 * @formatter:off
	 * Case : If the src-desc is defined as:
	 * "src-desc": {
	 *   "input1" : "topic1:par1,par2,par3"
	 *   "input2" : "topic1:par4"
	 *   "input3" : "topic2:par1"
	 * },
	 * a map with following entries will be returned.
	 * {
	 *   "topic1:par1", "input1"
	 *   "topic1:par2", "input1"
	 *   "topic1:par3", "input1"
	 *   "topic1:par4", "input2"
	 *   "topic2:par1", "input3"
	 * }
	 * @formatter:on
	 */
	@Test
	public void testGetDataSourceForTopicParitionInputs() throws AdaptorConfigurationException {
		SourceConfig sourceConfig = new SourceConfig();
		Map<String, Object> srcDescMap = new HashMap<>();
		srcDescMap.put("input1", "topic1:par1,par2,par3");
		srcDescMap.put("input2", "topic1:par4");
		srcDescMap.put("input3", "topic2:par1");
		sourceConfig.setSrcDesc(srcDescMap);

		HandlerConfig handlerConfig1 = Mockito.mock(HandlerConfig.class);
		LinkedHashSet<HandlerConfig> handlerConfigs = new LinkedHashSet<>();
		Mockito.when(handlerConfig1.getHandlerClass()).thenReturn("io.bigdime.core.handler.DummyConcreteHandler");

		handlerConfigs.add(handlerConfig1);
		sourceConfig.setHandlerConfigs(handlerConfigs);
		Collection<Source> sources = dataSourceFactory.getDataSource(sourceConfig);
		Assert.assertEquals(sources.size(), 5);
		// System.out.println(sources.iterator().next().getHandlers().iterator().next());
	}

	/*
	 * @formatter:off
	 * Case : If the src-desc is defined as:
	 * "src-desc": {
	 *   "input1" : "topic1:par1:par2"
	 * },
	 * 
	 * should throw InvalidValueConfigurationException as more than one colons are not allowed in input value.
	 * @formatter:on
	 */
	@Test
	public void testGetDataSourceForTopicParitionInputsWithMoreThanOneColon() throws AdaptorConfigurationException {
		SourceConfig sourceConfig = new SourceConfig();
		Map<String, Object> srcDescMap = new HashMap<>();
		srcDescMap.put("input1", "topic1:par1:par2");
		sourceConfig.setSrcDesc(srcDescMap);

		HandlerConfig handlerConfig1 = Mockito.mock(HandlerConfig.class);
		LinkedHashSet<HandlerConfig> handlerConfigs = new LinkedHashSet<>();
		Mockito.when(handlerConfig1.getHandlerClass()).thenReturn("io.bigdime.core.handler.DummyConcreteHandler");

		handlerConfigs.add(handlerConfig1);
		sourceConfig.setHandlerConfigs(handlerConfigs);
		dataSourceFactory.getDataSource(sourceConfig);
	}

	/*
	 * @formatter:off
	 * Case 3: If the src-desc is defined as:
	 * "src-desc": {
	 *   "input1" : "topic1:"
	 * },
	 * a map with following entries will be returned.
	 * {
	 *   "topic1", "input1"
	 * }
	 * @formatter:on
	 */

	@Test
	public void testGetDataSourceForTopicAndBlankParitionInputs() throws AdaptorConfigurationException {
		SourceConfig sourceConfig = new SourceConfig();
		Map<String, Object> srcDescMap = new HashMap<>();
		srcDescMap.put("input1", "topic1:");
		sourceConfig.setSrcDesc(srcDescMap);

		HandlerConfig handlerConfig1 = Mockito.mock(HandlerConfig.class);
		LinkedHashSet<HandlerConfig> handlerConfigs = new LinkedHashSet<>();
		Mockito.when(handlerConfig1.getHandlerClass()).thenReturn("io.bigdime.core.handler.DummyConcreteHandler");

		handlerConfigs.add(handlerConfig1);
		sourceConfig.setHandlerConfigs(handlerConfigs);
		Collection<Source> sources = dataSourceFactory.getDataSource(sourceConfig);
		Assert.assertEquals(sources.size(), 1);
	}

	/*
	 * @formatter:off
	 * Case : If the src-desc is defined as:
	 * "src-desc": {
	 *   "input1" : "table1,table2"
	 * },
	 * a map with following entries will be returned.
	 * {
	 *   "table1", "input1",
	 *   "table2", "input1"
	 * }
	 * @formatter:on
	 */
	@Test
	public void testGetDataSourceForStringInputs() throws AdaptorConfigurationException {
		SourceConfig sourceConfig = new SourceConfig();
		Map<String, Object> srcDescMap = new HashMap<>();
		srcDescMap.put("input1", "table1,table2");
		sourceConfig.setSrcDesc(srcDescMap);

		HandlerConfig handlerConfig1 = new HandlerConfig();// Mockito.mock(HandlerConfig.class);
		LinkedHashSet<HandlerConfig> handlerConfigs = new LinkedHashSet<>();
		handlerConfig1.setHandlerClass("io.bigdime.core.handler.DummyConcreteHandler");
		Map<String, Object> properties = new HashMap<>();
		handlerConfig1.setHandlerProperties(properties);
		// Mockito.when(handlerConfig1.getHandlerClass()).thenReturn("io.bigdime.core.handler.DummyConcreteHandler");

		handlerConfigs.add(handlerConfig1);
		sourceConfig.setHandlerConfigs(handlerConfigs);
		Collection<Source> sources = dataSourceFactory.getDataSource(sourceConfig);
		Assert.assertEquals(sources.size(), 2);
		Iterator<Source> iter = sources.iterator();// .next();//.getHandlers().iterator();
		String srcDescName = (((DummyConcreteHandler) (iter.next().getHandlers().iterator().next()))
				.getInputSrcDescName());
		System.out.println(srcDescName);
		Assert.assertTrue(srcDescName.equals("table1") || srcDescName.equals("table2"));
		srcDescName = (((DummyConcreteHandler) (iter.next().getHandlers().iterator().next())).getInputSrcDescName());
		Assert.assertTrue(srcDescName.equals("table1") || srcDescName.equals("table2"));
	}

	/**
	 * Assert that getDataSource throws an AdaptorConfigurationException if one
	 * of the inputs in src-desc is empty.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test(expectedExceptions = InvalidValueConfigurationException.class)
	public void testGetDataSourceWithBlankStringInputs() throws AdaptorConfigurationException {
		SourceConfig sourceConfig = new SourceConfig();
		Map<String, Object> srcDescMap = new HashMap<>();
		srcDescMap.put("input1", "");
		sourceConfig.setSrcDesc(srcDescMap);

		HandlerConfig handlerConfig1 = new HandlerConfig();// Mockito.mock(HandlerConfig.class);
		LinkedHashSet<HandlerConfig> handlerConfigs = new LinkedHashSet<>();
		handlerConfig1.setHandlerClass("io.bigdime.core.handler.DummyConcreteHandler");
		Map<String, Object> properties = new HashMap<>();
		handlerConfig1.setHandlerProperties(properties);

		handlerConfigs.add(handlerConfig1);
		sourceConfig.setHandlerConfigs(handlerConfigs);
		dataSourceFactory.getDataSource(sourceConfig);
	}
}

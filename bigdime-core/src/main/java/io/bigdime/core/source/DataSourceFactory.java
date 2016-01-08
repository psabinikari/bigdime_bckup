/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.source;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.Handler;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.Source;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.DescriptorParser;
import io.bigdime.core.commons.DescriptorParserFactory;
import io.bigdime.core.config.HandlerConfig;
import io.bigdime.core.config.SourceConfig;
import io.bigdime.core.handler.HandlerFactory;

/**
 * Factory to create DataSource object for the given {@link SourceConfig}
 * object.
 *
 * @author Neeraj Jain
 *
 */
@Component
public final class DataSourceFactory {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(DataSourceFactory.class));
	private static final String SRC_DESC = "src-desc";
	@Autowired
	private HandlerFactory handlerFactory;

	public Collection<Source> getDataSource(SourceConfig sourceConfig) throws AdaptorConfigurationException {

		if (sourceConfig == null) {
			throw new AdaptorConfigurationException("sourceConfig can't be null");
		}
		Collection<Source> dataSources = new HashSet<>();
		int sourceInstance = 0;
		Map<Object, String> descInputEntry = SrcDescReader.getDescriptorEntryMap(sourceConfig);
		for (Entry<Object, String> entry : descInputEntry.entrySet()) {
			sourceInstance++;
			final DataSource dataSource = new DataSource(getHandlers(sourceConfig.getHandlerConfigs(), entry),
					sourceConfig.getName() + "-" + sourceInstance);
			dataSource.setDescription(sourceConfig.getDescription());
			dataSources.add(dataSource);
		}
		return dataSources;
	}

	private LinkedHashSet<Handler> getHandlers(final Set<HandlerConfig> handlerConfigs,
			Entry<Object, String> descInputEntry) throws AdaptorConfigurationException {
		final LinkedHashSet<Handler> handlers = new LinkedHashSet<>();
		// final LinkedHashSet<HandlerConfig> handlerConfigs =
		// sourceConfig.getHandlerConfigs();
		int index = 0;
		for (HandlerConfig handlerConfig : handlerConfigs) {
			logger.debug("building source", "adding handler, handler_name=\"{}\"", handlerConfig.getName());
			handlerConfig.getHandlerProperties().put(SRC_DESC, descInputEntry);
			final Handler handler = handlerFactory.getHandler(handlerConfig);
			handler.setIndex(index);
			handlers.add(handler);
			index++;
		}
		return handlers;
	}

	private static class SrcDescReader {
		/**
		 * @formatter:off
		 * Case 1: If the src-desc is defined as:
		 * "src-desc": {
		 *   "input1" : "topic1:par1,par2,par3,"
		 * }, then
		 * a map with following entries will be returned.
		 * {"topic1:par1", "input1"
		 * "topic1:par2", "input1"
		 * "topic1:par3", "input1"}
		 * 
		 * Case 2: If the src-desc is defined as:
		 * "src-desc": {
		 *   "input1" : "topic1:par1,par2,par3,"
		 *   "input2" : "topic1:par4",
		 *   "input3" : "topic2:par1"
		 * },
		 * a map with following entries will be returned.
		 * {"topic1:par1", "input1"
		 * "topic1:par2", "input1"
		 * "topic1:par3", "input1"
		 * "topic1:par4", "input2"
		 * "topic2:par1", "input3"
		 * }
		 * 
		 * Case 3: If the src-desc is defined as:
		 * "src-desc": {
		 *   "input1" : "topic1:"
		 * },
		 * a map with following entries will be returned.
		 * {"topic1", "input1"
		 * }
		 * 
		 * @formatter:on
		 * @param sourceConfig
		 * @return
		 * @throws InvalidValueConfigurationException 
		 */
		private static Map<Object, String> getDescriptorEntryMap(SourceConfig sourceConfig)
				throws InvalidValueConfigurationException {
			Map<Object, String> descInputEntry = new HashMap<>();
			Map<String, Object> srcDesc = sourceConfig.getSrcDesc();
			try {
				for (Entry<String, Object> input : srcDesc.entrySet()) {
					Object descriptorNode = input.getValue();
					DescriptorParser parser = DescriptorParserFactory.getDescriptorParser(descriptorNode);
					/*
				 * @formatter:off
				 * {{entity-name=entityNameValue, topic=topic1, partition=part1}, input1}
				 * {some-value, input2}
				 * @formatter:on
				 */
					Map<Object, String> tempDescInputEntry = parser.parseDescriptor(input.getKey(), descriptorNode);
					descInputEntry.putAll(tempDescInputEntry);
				}
			} catch (IllegalArgumentException ex) {
				throw new InvalidValueConfigurationException(ex.getMessage());
			}
			return descInputEntry;
		}
	}
}

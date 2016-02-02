/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.Handler;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.StringHelper;
import io.bigdime.core.config.HandlerConfig;

@Component
public final class HandlerFactory {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(HandlerFactory.class));
	@Autowired
	private ApplicationContext context;
	private Properties appProperties;
	@Autowired
	private StringHelper stringHelper;

	public HandlerFactory() throws AdaptorConfigurationException {
		String envProperties = System.getProperty("env.properties");
		logger.info("constructing HandlerFactory", "envProperties=\"{}\"", envProperties);
		try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(envProperties)) {
		//try (InputStream is = new FileInputStream(envProperties)) {	
		appProperties = new Properties();
			appProperties.load(is);
			logger.info("constructing HandlerFactory", "properties=\"{}\"", appProperties.toString());
		} catch (IOException e) {
			logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
					ALERT_SEVERITY.BLOCKER, e.toString());
			throw new AdaptorConfigurationException(e);
		}
	}

	public Handler getHandler(final HandlerConfig handlerConfig) throws AdaptorConfigurationException {
		try {
			final Class<? extends Handler> handlerClass = Class.forName(handlerConfig.getHandlerClass())
					.asSubclass(Handler.class);
			Handler handler = context.getBean(handlerClass);

			Map<String, Object> handlerProperties = handlerConfig.getHandlerProperties();
			for (Entry<String, Object> handlerProperty : handlerProperties.entrySet()) {
				logger.info("building handler", "handler_property_name=\"{}\" value=\"{}\" isString=\"{}\"",
						handlerProperty.getKey(), handlerProperty.getValue(),
						(handlerProperty.getValue() instanceof String));
				if (handlerProperty.getValue() instanceof String) {
					String propValue = handlerProperty.getValue().toString();
					String newValue = stringHelper.redeemToken(propValue, appProperties);
					if (!propValue.equals(newValue)) {
						handlerProperty.setValue(newValue);
						logger.info("building handler",
								"handler_property_name=\"{}\" old_value=\"{}\" new_value=\"{}\"",
								handlerProperty.getKey(), propValue, newValue);
					}
				}
			}
			handler.setPropertyMap(handlerConfig.getHandlerProperties());
			handler.setName(handlerConfig.getName());
			handler.build();
			logger.debug("building handler", "handler_name=\"{}\" handler_properties=\"{}\"", handler.getName(),
					handlerConfig.getHandlerProperties());
			return handler;
		} catch (ClassNotFoundException e) {
			logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION,
					ALERT_SEVERITY.BLOCKER, e.toString());
			throw new AdaptorConfigurationException(e);
		}
	}
}

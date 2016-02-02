/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.AdaptorConfigConstants.ValidationHandlerConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.core.validation.Validator;
import io.bigdime.core.validation.ValidatorFactory;

/**
 * @formatter:off
 * Data validation handler that uses one or more validators to perform
 * validation. data-validation-handler needs to be configured with
 * validation_type property to inject one or more validators; e.g.
 * "properties": {
 *    "validation_type" : "raw_checksum | record_count"
 * }
 * @formatter:on
 * 
 * @author Neeraj Jain
 *
 */
@Component
@Scope("prototype")
public class DataValidationHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(DataValidationHandler.class));

	@Autowired
	private ValidatorFactory validatorFactory;
	private List<Validator> validators = new ArrayList<>();
	private String hiveHostName;
	private String hivePort;
	private String haEnabled;
	private String hiveProxyProvider;
	private String haServiceName;
	private String dfsNameService;
	private String dfsNameNode1;
	private String dfsNameNode2;
	
	@Autowired
	private RuntimeInfoStore<RuntimeInfo> runtimeInfoStore;

	/**
	 * Read the validation_type property, get corresponding Validators and add
	 * them to the list.
	 */
	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		logger.debug("building validation handler", "property_map=\"{}\"", getPropertyMap());

		final String validationTypeProperty = (String) getPropertyMap()
				.get(ValidationHandlerConfigConstants.VALIDATION_TYPE);
		final String[] validationTypes = validationTypeProperty.split("\\|");
		for (String type : validationTypes) {
			type = type.trim();
			logger.debug("building validation handler", "validation_type=\"{}\"", type);
			validators.add(validatorFactory.getValidator(type.trim()));
		}
		hiveHostName = (String) getPropertyMap()
				.get(ValidationHandlerConfigConstants.HIVE_HOST);
		hivePort = (String) getPropertyMap()
				.get(ValidationHandlerConfigConstants.HIVE_PORT);
		haEnabled = (String) getPropertyMap()
				.get(ValidationHandlerConfigConstants.HA_ENABLED);
		hiveProxyProvider = (String) getPropertyMap()
				.get(ValidationHandlerConfigConstants.DFS_CLIENT_FAILOVER_PROVIDER);
		haServiceName = (String) getPropertyMap()
				.get(ValidationHandlerConfigConstants.HA_SERVICE_NAME);
		dfsNameService = (String) getPropertyMap()
				.get(ValidationHandlerConfigConstants.DFS_NAME_SERVICES);
		dfsNameNode1 = (String) getPropertyMap()
				.get(ValidationHandlerConfigConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE1);
		dfsNameNode2 = (String) getPropertyMap()
				.get(ValidationHandlerConfigConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE2);
	}

	@Override
	public Status process() throws HandlerException {
		logger.debug("DataValidationHandler processing event", "DataValidationHandler processing event");
		List<ActionEvent> actionEvents = getHandlerContext().getEventList();
		Preconditions.checkNotNull(actionEvents, "eventList in HandlerContext can't be null");
		logger.debug("DataValidationHandler processing event", "actionEvents.size=\"{}\"", actionEvents.size());
		Preconditions.checkArgument(!actionEvents.isEmpty(), "eventList in HandlerContext can't be empty");
		ActionEvent actionEvent = actionEvents.get(0);
		if(actionEvent != null){
			actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, hiveHostName);
			actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, hivePort);
			actionEvent.getHeaders().put(ValidationHandlerConfigConstants.HA_ENABLED, haEnabled);
			actionEvent.getHeaders().put(ValidationHandlerConfigConstants.DFS_CLIENT_FAILOVER_PROVIDER, hiveProxyProvider);
			actionEvent.getHeaders().put(ValidationHandlerConfigConstants.HA_SERVICE_NAME, haServiceName);
			actionEvent.getHeaders().put(ValidationHandlerConfigConstants.DFS_NAME_SERVICES, dfsNameService);
			actionEvent.getHeaders().put(ValidationHandlerConfigConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE1, dfsNameNode1);
			actionEvent.getHeaders().put(ValidationHandlerConfigConstants.DFS_NAME_NODE_RPC_ADDRESS_NODE2, dfsNameNode2);
		}
		
		process0(actionEvent);
		
		return Status.READY;
	}

	private void process0(ActionEvent actionEvent) throws HandlerException {
		logger.debug("DataValidationHandler processing event", "actionEvent==null=\"{}\"", actionEvent == null);
		boolean validationPassed = false;
		if (actionEvent == null) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.VALIDATION_ERROR, ALERT_SEVERITY.BLOCKER,
					"_message=\"validation failed, null ActionEvent found in HandlerContext\"");
			throw new ValidationHandlerException("validation failed, null ActionEvent found in HandlerContext");
		}

		for (final Validator validator : validators) {
			try {
				ValidationResponse validationResponse = validator.validate(actionEvent);

				if (validationResponse.getValidationResult() != ValidationResult.NOT_READY) {
					validationPassed = validationResponse.getValidationResult() == ValidationResult.PASSED;
					logger.debug("DataValidationHandler processing event", "updating runtime info");
					updateRuntimeInfoToStoreAfterValidation(runtimeInfoStore, validationPassed, actionEvent);
					logger.debug("DataValidationHandler processing event", "updated runtime info");
				} else {
					logger.debug("DataValidationHandler processing event", "validation was skipped");
				}
			} catch (DataValidationException e) {
				logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.VALIDATION_ERROR, ALERT_SEVERITY.BLOCKER,
						"_message=\"validation failed\" validator_name=\"{}\"", validator.getName(), e);
				throw new ValidationHandlerException("validation failed, validator_name=" + validator.getName(), e);
			} catch (Exception e) {
				logger.debug("exception during validation", "src_desc=\"{}\"",
						actionEvent.getHeaders().get("src-desc"));
				throw new HandlerException(e.getMessage(), e);
			}
		}
	}
}

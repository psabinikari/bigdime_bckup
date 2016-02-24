/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.webhdfs;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
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
import io.bigdime.core.InvalidDataException;
import io.bigdime.core.SinkHandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants.SinkConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.handler.constants.WebHDFSWriterHandlerConstants;
import io.bigdime.libs.hdfs.WebHDFSConstants;
import io.bigdime.libs.hdfs.WebHdfs;
import io.bigdime.libs.hdfs.WebHdfsWriter;

/**
 * WebHDFSWriterHandler implements process method to write to HDFS.
 * 
 * @author jbrinnand, Neeraj Jain
 *
 */
@Component
@Scope("prototype")
public class WebHDFSWriterHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(WebHDFSWriterHandler.class));

	private String hostNames;
	private int port;
	private String hdfsFileName;
	private String hdfsFileNamePrefix = "";
	private String hdfsFileNameExtension = "";
	private String hdfsPath;
	private String hdfsUser;
	private String hdfsOverwrite;
	private String hdfsPermission;
	private WebHdfs webHdfs;
	@Autowired
	private RuntimeInfoStore<RuntimeInfo> runtimeInfoStore;

	private String handlerPhase;
	private String channelDesc;
	/**
	 * hdfsPath can be tokenized, e.g.
	 * /webhdfs/v1/data/unit/${account}/${timestamp}. Here, ${account} should be
	 * replaced with a value from ACCOUNT header and ${timestamp} should be
	 * replaced with a value from TIMESTAMP header.
	 */
	private Map<String, String> tokenToHeaderNameMap;

	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		try {
			handlerPhase = "building WebHDFSWriterHandler";
			logger.info(handlerPhase, "building WebHDFSWriterHandler");
			hostNames = PropertyHelper.getStringProperty(getPropertyMap(), WebHDFSWriterHandlerConstants.HOST_NAMES);
			port = PropertyHelper.getIntProperty(getPropertyMap(), WebHDFSWriterHandlerConstants.PORT);
			// hdfsFileName = PropertyHelper.getStringProperty(getPropertyMap(),
			// WebHDFSWriterHandlerConstants.HDFS_FILE_NAME);
			hdfsFileNamePrefix = PropertyHelper.getStringProperty(getPropertyMap(),
					WebHDFSWriterHandlerConstants.HDFS_FILE_NAME_PREFIX, "");
			hdfsFileNameExtension = PropertyHelper.getStringProperty(getPropertyMap(),
					WebHDFSWriterHandlerConstants.HDFS_FILE_NAME_EXTENSION, "");

			channelDesc = (String) getPropertyMap().get(SinkConfigConstants.CHANNEL_DESC);

			// hdfsFileName = hdfsFileNamePrefix + "-" + channelDesc +
			// hdfsFileNameExtension;
			// hdfsFileName = new
			// HdfsFileNameBuilder().withPrefix(hdfsFileNamePrefix).withChannelDesc(channelDesc)
			// .withExtension(hdfsFileNameExtension).build();

			logger.info(handlerPhase, "hdfsFileNamePrefix={} hdfsFileNameExtension={} channelDesc={} hdfsFileName={}",
					hdfsFileNamePrefix, hdfsFileNameExtension, channelDesc, hdfsFileName);
			hdfsPath = PropertyHelper.getStringProperty(getPropertyMap(), WebHDFSWriterHandlerConstants.HDFS_PATH);
			hdfsUser = PropertyHelper.getStringProperty(getPropertyMap(), WebHDFSWriterHandlerConstants.HDFS_USER);
			logger.info(handlerPhase, "hdfsUser={}", hdfsUser);

			hdfsPermission = PropertyHelper.getStringProperty(getPropertyMap(),
					WebHDFSWriterHandlerConstants.HDFS_PERMISSIONS);
			hdfsOverwrite = PropertyHelper.getStringProperty(getPropertyMap(),
					WebHDFSWriterHandlerConstants.HDFS_OVERWRITE);
			// webHdfs = WebHdfs.getInstance(hostNames, port);
			tokenToHeaderNameMap = new LinkedHashMap<>();
			Pattern p = Pattern.compile("\\$\\{(\\w+)\\}+");
			Matcher m = p.matcher(hdfsPath);

			while (m.find()) {
				String token = m.group();// e.g. token=${account}
				String headerName = m.group(1);// e.g.
				// headerName=account
				tokenToHeaderNameMap.put(token, headerName);
			}

			logger.info("building WebHDFSWriterHandler",
					"hostNames={} port={} hdfsFileName={} hdfsPath={} hdfsUser={} hdfsPermission={} hdfsOverwrite={}",
					hostNames, port, hdfsFileName, hdfsPath, hdfsUser, hdfsPermission, hdfsOverwrite);
		} catch (final Exception ex) {
			throw new AdaptorConfigurationException(ex);
		}
	}

	/**
	 * Use WebHdfsWriter component to write to HDFS. It also replaces tokens in
	 * hdfsPath with appropriate values from event's header. If the header does
	 * not contain the required keys needed to replace tokens or contains a null
	 * value, an {@link InvalidDataException} is thrown. If the WebHdfsWriter is
	 * unable to write data to HDFS, SinkHandlerException is thrown.
	 */

	/*
	 * Process one or more events and returns the status. 
	 * @formatter:off
	 * Reads 100 messages, say 70 messages are from hour=10 and 30 are from hour=11. (it can find out from header).
	 * Puts 70 messages from hour=10 in one actionevent. 
	 * Sets a header saying "EndOfFile" or something indicating that it's ready for
	 * validation. 
	 * Journal rest 30. 
	 * Return actionevent for next handler to process.
	 * 
	 * 
	 * 
	 * @formatter:on
	 */
	@Override
	public Status process() throws HandlerException {
		handlerPhase = "processing WebHDFSWriterHandler";
		logger.info(handlerPhase, "webHdfs processing event");
		WebHDFSWriterHandlerJournal journal = getJournal(WebHDFSWriterHandlerJournal.class);

		if (journal == null || journal.getEventList() == null) {
			// process for ready status.
			List<ActionEvent> actionEvents = getHandlerContext().getEventList();
			Preconditions.checkNotNull(actionEvents, "eventList in HandlerContext can't be null");
			logger.info(handlerPhase, "journal is null, actionEvents.size={} id={} ", actionEvents.size(), getId());

			return process0(actionEvents);

		} else {
			List<ActionEvent> actionEvents = journal.getEventList();

			logger.info(handlerPhase, "journal is not null, actionEvents==null={}", (actionEvents == null));
			if (actionEvents != null && !actionEvents.isEmpty()) {
				// process for CALLBACK status.
				return process0(journal.getEventList());
			}
		}
		return null;
	}

	private String getPreviousHdfsPath(WebHDFSWriterHandlerJournal journal) {
		return journal.getCurrentHdfsPath();
	}

	private String getPreviousHdfsPathWithName(WebHDFSWriterHandlerJournal journal) {
		return journal.getCurrentHdfsPathWithName();
	}

	private String getPreviousHdfsFileName(WebHDFSWriterHandlerJournal journal) {
		return journal.getCurrentHdfsFileName();
	}

	private void buildFileName(ActionEvent actionEvent) {
		String sourceFileNameFromHeader = actionEvent.getHeaders().get(ActionEventHeaderConstants.SOURCE_FILE_NAME);
		hdfsFileName = new HdfsFileNameBuilder().withChannelDesc(channelDesc).withPrefix(hdfsFileNamePrefix)
				.withSourceFileName(sourceFileNameFromHeader).withExtension(hdfsFileNameExtension).build();
	}

	private Status process0(List<ActionEvent> actionEvents) throws HandlerException {
		WebHDFSWriterHandlerJournal journal = getJournal(WebHDFSWriterHandlerJournal.class);
		if (journal == null) {
			logger.debug(handlerPhase, "jounral is null, initializing");
			journal = new WebHDFSWriterHandlerJournal();
			getHandlerContext().setJournal(getId(), journal);
		}

		logger.debug(handlerPhase, "previousHdfsPath={}", getPreviousHdfsPath(journal));
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		Status statusToReturn = Status.READY;
		try {
			Iterator<ActionEvent> actionEventIter = actionEvents.iterator();
			boolean payloadEmpty = true;
			// HdfsPathHelper hdfsPathHelper = new HdfsPathHelper();
			String detokenizedHdfsPath = null;
			String detokenizedHdfsPathWithName = null;
			ActionEvent actionEvent = null;
			ActionEvent prevActionEvent = null;
			HdfsFilePathBuilder hdfsFilePathBuilder = null;
			boolean validateCurrentActionEvents = false;
			boolean validatePreviousPartitionActionEvents = false;
			while (actionEventIter.hasNext()) {
				actionEvent = actionEventIter.next();
				buildFileName(actionEvent);
				hdfsFilePathBuilder = new HdfsFilePathBuilder();
				detokenizedHdfsPath = hdfsFilePathBuilder.withActionEvent(actionEvent).withHdfsPath(hdfsPath)
						.withTokenHeaderMap(tokenToHeaderNameMap).build();

				actionEvent.getHeaders().put(ActionEventHeaderConstants.HDFS_PATH,
						hdfsFilePathBuilder.getBaseHdfsPath());// needed
				boolean cleanupRequired = actionEvent.getHeaders()
						.get(ActionEventHeaderConstants.CLEANUP_REQUIRED) != null
						&& actionEvent.getHeaders().get(ActionEventHeaderConstants.CLEANUP_REQUIRED)
								.equalsIgnoreCase("true");
				if (cleanupRequired) {
					logger.debug(handlerPhase, "\"will cleanuphdfs dir={}\"", actionEvent.getHeaders());

					handleExceptionCondition(actionEvent);
				}
				if (detokenizedHdfsPath.endsWith(File.separator)) {
					detokenizedHdfsPathWithName = detokenizedHdfsPath + hdfsFileName;
				} else {
					detokenizedHdfsPathWithName = detokenizedHdfsPath + File.separator + hdfsFileName;
				}

				/*
				 * @formatter:off
				 * If previousHdfsPath is blank or previousHdfsPath is same as new path 
				 * 	add to payload 
				 * 	increment record count 
				 * 	remove from list 
				 * 	set payloadEmpty=false 
				 * else 
				 * 	write to hdfs 
				 * 	write header
				 * 	set journal
				 * @formatter:on
				 */
				logger.debug(handlerPhase,
						"previousHdfsPath={} detokenizedHdfsPath={} previousHdfsPathWithName={} detokenizedHdfsPathWithName={} previousHdfsFileName={}",
						getPreviousHdfsPath(journal), detokenizedHdfsPath, getPreviousHdfsPathWithName(journal),
						detokenizedHdfsPathWithName, getPreviousHdfsFileName(journal));
				if (StringUtils.isBlank(getPreviousHdfsPathWithName(journal))
						|| detokenizedHdfsPathWithName.equals(getPreviousHdfsPathWithName(journal))) {
					logger.debug(handlerPhase, "appending payload, record_count={} actionEvents.size={}",
							journal.getRecordCount(), actionEvents.size());
					payload.write(actionEvent.getBody());
					actionEventIter.remove();
					payloadEmpty = false;
					if (actionEvent.getHeaders().get(ActionEventHeaderConstants.VALIDATION_READY) != null
							&& actionEvent.getHeaders().get(ActionEventHeaderConstants.VALIDATION_READY).equalsIgnoreCase("true"))
						validateCurrentActionEvents = true;
					else{
					initializeRecordCountInJournal(actionEvent, journal);
					journal.incrementRecordCount();
					
					prevActionEvent = actionEvent;
					logger.debug(handlerPhase, "appended payload, record_count={} actionEvents.size={}",
							journal.getRecordCount(), actionEvents.size());
					}
				} else {
					logger.debug(handlerPhase, "new hdfspath, payloadEmpty={} hdfsFileName={} previousHdfsFileName={}",
							payloadEmpty, hdfsFileName, getPreviousHdfsFileName(journal));
					if (!payloadEmpty) {
						logger.info(handlerPhase, "writing to hdfs, validation should be performed");
						prevActionEvent.getHeaders().put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.TRUE.toString());
						validatePreviousPartitionActionEvents = true;
						/*ActionEvent returnEvent = writeToHdfs(getPreviousHdfsPath(journal), payload.toByteArray(),
								getPreviousHdfsFileName(journal), hdfsFilePathBuilder, prevActionEvent);
						payloadEmpty = true;
						journal.setEventList(actionEvents);
						logger.info(handlerPhase, "setting event in journal, actionEvents.size={}",
								actionEvents.size());
						statusToReturn = Status.CALLBACK;
						getHandlerContext().createSingleItemEventList(returnEvent);

						journal.setCurrentHdfsPath(detokenizedHdfsPath);
						journal.setCurrentHdfsPathWithName(detokenizedHdfsPathWithName);
						journal.setCurrentHdfsFileName(hdfsFileName);
						journal.setRecordCount(0);
						break;*/
					}
				}
				if(validatePreviousPartitionActionEvents || validateCurrentActionEvents) {
					if(!payloadEmpty){
						ActionEvent returnEvent = null;
						if(validatePreviousPartitionActionEvents)
							returnEvent = writeToHdfs(getPreviousHdfsPath(journal), payload.toByteArray(),
									getPreviousHdfsFileName(journal), hdfsFilePathBuilder, prevActionEvent);
						else
						 returnEvent = writeToHdfs(getPreviousHdfsPath(journal), payload.toByteArray(),
								getPreviousHdfsFileName(journal), hdfsFilePathBuilder, actionEvent);
						payloadEmpty = true;
						journal.setEventList(actionEvents);
						logger.info(handlerPhase, "setting event in journal, actionEvents.size={}",
								actionEvents.size());
						statusToReturn = Status.CALLBACK;
						getHandlerContext().createSingleItemEventList(returnEvent);

						journal.setCurrentHdfsPath(detokenizedHdfsPath);
						journal.setCurrentHdfsPathWithName(detokenizedHdfsPathWithName);
						journal.setCurrentHdfsFileName(hdfsFileName);
						journal.setRecordCount(0);
						break;
					}
						
					
				}
				if (StringUtils.isBlank(getPreviousHdfsPathWithName(journal))) {
					journal.setCurrentHdfsPath(detokenizedHdfsPath);
					journal.setCurrentHdfsPathWithName(detokenizedHdfsPathWithName);
					journal.setCurrentHdfsFileName(hdfsFileName);
				}
			}
			if (actionEvent != null && actionEvents.isEmpty()) {
				if (!payloadEmpty) {
					logger.info(handlerPhase,
							"writing to hdfs. previousHdfsPath={} detokenizedHdfsPath={} previousHdfsPathWithName={} detokenizedHdfsPathWithName={}",
							getPreviousHdfsPath(journal), detokenizedHdfsPath, getPreviousHdfsPathWithName(journal),
							detokenizedHdfsPathWithName);
					//actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, Boolean.FALSE.toString());
					if(actionEvent.getHeaders().get(ActionEventHeaderConstants.VALIDATION_READY) == null)
					actionEvent.getHeaders().put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.FALSE.toString());
					ActionEvent returnEvent = writeToHdfs(detokenizedHdfsPath, payload.toByteArray(), hdfsFileName,
							hdfsFilePathBuilder, actionEvent);
					getHandlerContext().createSingleItemEventList(returnEvent);
				}
				logger.info(handlerPhase, "clearing the journal ", actionEvents.size(), getId());

				getHandlerContext().clearJournal(getId());
			}
		} catch (IOException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
					"\"IOException received\" error={}", e.toString());
			throw new SinkHandlerException("WebHDFSWriterHandler processing event failed", e);
		} catch (Exception e) {
			throw new HandlerException(e.getMessage(), e);
		}
		logger.debug(handlerPhase, "statusToReturn={}", statusToReturn);
		return statusToReturn;
	}

	private ActionEvent writeToHdfs(String hdfsPath12, byte[] payload, String hdfsFileName,
			HdfsFilePathBuilder hdfsFilePathBuilder1, ActionEvent inputEvent)
					throws IOException, RuntimeInfoStoreException, HandlerException {
		final WebHdfsWriter writer = new WebHdfsWriter();
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		String detokenizedHdfsPath = hdfsFilePathBuilder.withActionEvent(inputEvent).withHdfsPath(hdfsPath)
				.withTokenHeaderMap(tokenToHeaderNameMap).build();
		WebHDFSWriterHandlerJournal journal = getJournal(WebHDFSWriterHandlerJournal.class);
		logger.debug(handlerPhase, "journal={}", journal);
		logger.debug(handlerPhase, "_message=\"writing to hdfs\" hdfsPath={} hdfsFileName={} hdfsUser={}",
				detokenizedHdfsPath, hdfsFileName, hdfsUser);
		if (webHdfs == null) {
			webHdfs = WebHdfs.getInstance(hostNames, port)
					.addHeader(WebHDFSConstants.CONTENT_TYPE, WebHDFSConstants.APPLICATION_OCTET_STREAM)
					.addParameter(WebHDFSConstants.USER_NAME, hdfsUser);
		}
		writer.write(webHdfs, detokenizedHdfsPath, payload, hdfsFileName);
		webHdfs = null;
		logger.debug(handlerPhase, "wrote to hdfs");
		String partitionNames = null;
		String partitionValues = null;
		for (Entry<String, String> hivePartitionNameValue : hdfsFilePathBuilder.getPartitionNameValueMap().entrySet()) {
			
			if(!isNonPartitionKey(inputEvent,hivePartitionNameValue.getKey()))
				if (partitionValues == null) {
					partitionValues = hivePartitionNameValue.getValue();
					partitionNames = hivePartitionNameValue.getKey();
				} else {
					partitionValues += "," + hivePartitionNameValue.getValue();
					partitionNames += "," + hivePartitionNameValue.getKey();
				}
		}
		logger.info(handlerPhase, "partitionNames={} partitionValues={} hdfsFileName={}", partitionNames,
				partitionValues, hdfsFileName);
		ActionEvent actionEvent = new ActionEvent(inputEvent);

		Map<String, String> headers = new HashMap<>(inputEvent.getHeaders());

		actionEvent.setHeaders(headers);
		headers.put(ActionEventHeaderConstants.HIVE_PARTITION_NAMES, partitionNames);
		headers.put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, partitionValues);
		headers.put(ActionEventHeaderConstants.HDFS_FILE_NAME, hdfsFileName);
		headers.put(ActionEventHeaderConstants.RECORD_COUNT, String.valueOf(journal.getRecordCount()));
		headers.put(ActionEventHeaderConstants.COMPLETE_HDFS_PATH, detokenizedHdfsPath + File.separator);
		headers.put(ActionEventHeaderConstants.HDFS_PATH, hdfsFilePathBuilder.getBaseHdfsPath());
		headers.put(ActionEventHeaderConstants.HOST_NAMES, hostNames);
		headers.put(ActionEventHeaderConstants.PORT, String.valueOf(port));
		headers.put(ActionEventHeaderConstants.USER_NAME, hdfsUser);
		logger.debug(handlerPhase, "headers_from_hdfswriter={}", headers);
		return actionEvent;

	}
	
	private boolean isNonPartitionKey(ActionEvent inputEvent,String hivePartitionName){
		
		boolean isNonPartition = false;
		String hiveNonPartitionNames = inputEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_NON_PARTITION_NAMES);
		if(hiveNonPartitionNames == null)
			return isNonPartition;
		
		if(hiveNonPartitionNames != null){
			String[] hiveNonPartitionStNames = hiveNonPartitionNames.split(",");
			for(String hiveNonPartitionName: hiveNonPartitionStNames)
				if(hivePartitionName.equalsIgnoreCase(hiveNonPartitionName))
					isNonPartition = true;
			
		}
		
		
		return isNonPartition;
		
	}
	
	private void initializeRecordCountInJournal(final ActionEvent actionEvent,
			final WebHDFSWriterHandlerJournal journal) throws RuntimeInfoStoreException {
		if (journal.getRecordCount() < 0) {
			String entityName = actionEvent.getHeaders().get(ActionEventHeaderConstants.ENTITY_NAME);
			String inputDescriptor = actionEvent.getHeaders().get(ActionEventHeaderConstants.INPUT_DESCRIPTOR);
			try {
				RuntimeInfo runtimeInfo = runtimeInfoStore.get(AdaptorConfig.getInstance().getName(), entityName,
						inputDescriptor);

				if (runtimeInfo != null && runtimeInfo.getProperties() != null
						&& runtimeInfo.getProperties().get(ActionEventHeaderConstants.RECORD_COUNT) != null) {
					int recordCountFromRuntimeInfo = PropertyHelper
							.getIntProperty(runtimeInfo.getProperties().get(ActionEventHeaderConstants.RECORD_COUNT));
					logger.debug(handlerPhase, "read from runtime_store, recordCount={}", recordCountFromRuntimeInfo);
					journal.setRecordCount(recordCountFromRuntimeInfo);
				} else {
					journal.setRecordCount(0);
				}
			} catch (IllegalArgumentException ex) {
				journal.setRecordCount(0);
			}
		}
	}

	protected String getHandlerPhase() {
		return handlerPhase;
	}

	@Override
	public void handleException() {
		// List<ActionEvent> actionEvents = getHandlerContext().getEventList();
		// Preconditions.checkNotNull(actionEvents, "eventList in HandlerContext
		// can't be null");
		// logger.debug("WebHDFSWriterHandler handling exception",
		// "actionEvents.size=\"{}\"", actionEvents.size());
		// Preconditions.checkArgument(!actionEvents.isEmpty(), "eventList in
		// HandlerContext can't be empty");
		// ActionEvent actionEvent = actionEvents.get(0);
		// handleExceptionCondition(actionEvent);
	}

	public void handleExceptionCondition(final ActionEvent actionEvent) {

		logger.info(handlerPhase,
				"\"cleaning up file from previous run\" hdfsFileNamePrefix={} hdfsFileNameExtension={} channelDesc={} hdfsFileName={}",
				hdfsFileNamePrefix, hdfsFileNameExtension, channelDesc, hdfsFileName);
		String fromPath = "";
		String toPath = "";
		String toPathDir = "";
		try {

			{
				String hdfsBasePath = actionEvent.getHeaders().get(ActionEventHeaderConstants.HDFS_PATH);

				String hivePartitionValues = actionEvent.getHeaders()
						.get(ActionEventHeaderConstants.HIVE_PARTITION_VALUES);

				logger.info(handlerPhase,
						"\"cleaning up file from previous run\" hdfsBasePath={} hivePartitionValues={}", hdfsBasePath,
						hivePartitionValues);

				String partitionPath = "";
				if (StringUtils.isNotBlank(hivePartitionValues)) {
					String[] partitionList = hivePartitionValues.split(",");
					StringBuilder stringBuilder = new StringBuilder();
					for (int i = 0; i < partitionList.length; i++) {
						stringBuilder.append(partitionList[i].trim() + "/");
					}
					partitionPath = stringBuilder.toString();
					fromPath = hdfsBasePath + partitionPath + hdfsFileName;
				} else {
					fromPath = hdfsBasePath + hdfsFileName;
				}

				String destinationFilePath = "backup/"
						+ AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName() + "/" + partitionPath + "/";

				// Take out /webhdfs/v1 from hdfsBasePath
				String hdfsDir = hdfsBasePath.substring(11);
				toPath = hdfsDir + destinationFilePath;
				toPathDir = hdfsBasePath + destinationFilePath;
			}
			if (webHdfs == null) {
				webHdfs = WebHdfs.getInstance(hostNames, port)
						.addHeader(WebHDFSConstants.CONTENT_TYPE, WebHDFSConstants.APPLICATION_OCTET_STREAM)
						.addParameter(WebHDFSConstants.USER_NAME, hdfsUser);
			}
			WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
			logger.info(handlerPhase, "\"creating directory\" toPath={}", toPath);
			webHdfsWriter.createDirectory(webHdfs, toPathDir);

			logger.info(handlerPhase, "\"moving file\" fromPath={} toPath={}", fromPath, toPath);
			moveErrorRecordCountFile(fromPath, toPath + hdfsFileName + "-" + System.currentTimeMillis());
			webHdfs = null;
		} catch (IOException e) {
			logger.warn(handlerPhase, "Exception occurs, Failed to move to provided location: toPath={}", toPath, e);
		}
	}

	private void moveErrorRecordCountFile(String source, String dest) throws IOException {
		webHdfs.addParameter("destination", dest);
		HttpResponse response = webHdfs.rename(source);
		if (response.getStatusLine().getStatusCode() == 200 || response.getStatusLine().getStatusCode() == 201) {
			logger.debug(handlerPhase, "\"file moved successfully\"", "responseCode={} dest={} responseMessage={}",
					response.getStatusLine().getStatusCode(), dest, response.getStatusLine().getReasonPhrase());
		} else if (response.getStatusLine().getStatusCode() == 404) {
			logger.debug(handlerPhase, "file does not exist", "responseCode={} dest={} responseMessage={}",
					response.getStatusLine().getStatusCode(), dest, response.getStatusLine().getReasonPhrase());
		} else {
			logger.warn(handlerPhase, "file existence not known, responseCode={} dest={} responseMessage={}",
					response.getStatusLine().getStatusCode(), dest, response.getStatusLine().getReasonPhrase());
		}
		webHdfs.releaseConnection();
	}
}
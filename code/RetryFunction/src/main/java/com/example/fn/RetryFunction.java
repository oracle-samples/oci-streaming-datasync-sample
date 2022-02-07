//Copyright (c)  2021,  Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

//This Function retries the messages in streams. This Function is exposed as a public API using an API Gateway. 
//The exposed API can be invoked as a batch process or on an ad-hoc basis,
//to reprocess the  messages in any  Stream.

package com.example.fn;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.core.Response.Status.Family;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fnproject.fn.api.httpgateway.HTTPGatewayContext;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.secrets.SecretsClient;
import com.oracle.bmc.secrets.model.Base64SecretBundleContentDetails;
import com.oracle.bmc.secrets.requests.GetSecretBundleByNameRequest;
import com.oracle.bmc.secrets.responses.GetSecretBundleByNameResponse;
import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.CreateCursorDetails;
import com.oracle.bmc.streaming.model.CreateCursorDetails.Type;
import com.oracle.bmc.streaming.model.Message;
import com.oracle.bmc.streaming.model.PutMessagesDetails;
import com.oracle.bmc.streaming.model.PutMessagesDetailsEntry;
import com.oracle.bmc.streaming.model.PutMessagesResultEntry;
import com.oracle.bmc.streaming.model.Stream;
import com.oracle.bmc.streaming.model.Stream.LifecycleState;
import com.oracle.bmc.streaming.requests.CreateCursorRequest;
import com.oracle.bmc.streaming.requests.GetMessagesRequest;
import com.oracle.bmc.streaming.requests.GetStreamRequest;
import com.oracle.bmc.streaming.requests.ListStreamsRequest;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import com.oracle.bmc.streaming.responses.CreateCursorResponse;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import com.oracle.bmc.streaming.responses.GetStreamResponse;
import com.oracle.bmc.streaming.responses.ListStreamsResponse;
import com.oracle.bmc.streaming.responses.PutMessagesResponse;

public class RetryFunction {
	private static final Logger LOGGER = Logger.getLogger(RetryFunction.class.getName());
	private final ResourcePrincipalAuthenticationDetailsProvider provider = ResourcePrincipalAuthenticationDetailsProvider
			.builder().build();
	private static final String VAULT_OCID = System.getenv().get("vault_ocid");
	private static final String STREAM_COMPARTMENT_OCID = System.getenv().get("stream_compartment_ocid");
	private static final String DEFAULT_ERROR_STREAM_OCID = System.getenv().get("default_error_stream_ocid");
	private static final String[] OPERATIONS = new String[] { "PUT", "POST", "DELETE" };

	/**
	 * @param requestBody
	 * @param httpGatewayContext
	 * @return String
	 * 
	 *         This is the entry point of the Function call
	 */
	public String handleRequest(String requestBody, HTTPGatewayContext httpGatewayContext)

	{

		Map<String, String> errorStreamMapping = new HashMap<>();
		ObjectMapper mapper = new ObjectMapper();
		StreamAdminClient streamAdminClient = StreamAdminClient.builder().build(provider);

		String readPartition = "";
		int noOfMessagesToProcess = 0;
		int readAfterOffset = 0;
		String streamOCIDToRetry = "";

		try {
			JsonNode jsonNode = mapper.readTree(requestBody);

			String[] keys = { "streamOCIDToRetry", "readAfterOffset", "readPartition", "noOfMessagesToProcess",
					"errormapping" };

			for (String key : keys) {
				if (!jsonNode.has(key)) {

					LOGGER.log(Level.SEVERE, "{0} doesnt exist.", key);

					httpGatewayContext.setStatusCode(500);
					return key + " doesnt exist.";
				}

			}

			streamOCIDToRetry = jsonNode.path("streamOCIDToRetry").asText();

			// check if the stream exists
			if (!streamExist(streamOCIDToRetry, streamAdminClient)) {
				LOGGER.log(Level.SEVERE,
						"Processing Failed.  Correct the streamOCIDToRetry with correct stream OCID. {0}  doesnt exist",
						streamOCIDToRetry);

				httpGatewayContext.setStatusCode(500);
				return streamOCIDToRetry + " doesnt exist. Correct the streamOCIDToRetry  with correct stream OCID";

			}

			readAfterOffset = jsonNode.path("readAfterOffset").asInt();

			readPartition = jsonNode.path("readPartition").asText();

			noOfMessagesToProcess = jsonNode.path("noOfMessagesToProcess").asInt();
			if (noOfMessagesToProcess <= 0) {
				LOGGER.log(Level.INFO, "Stopped Function execution as noOfMessagesToProcess <=0.");
				return "Stopped Function execution as noOfMessagesToProcess <=0.";

			}

			if (!streamExist(DEFAULT_ERROR_STREAM_OCID, streamAdminClient)) {

				LOGGER.log(Level.SEVERE, "Check DEFAULT_ERROR_STREAM_OCID value in Function Configurations.");
				httpGatewayContext.setStatusCode(500);
				return "Check DEFAULT_ERROR_STREAM_OCID value in Function Configurations.";
			}

			// Get the error mapping nodes to get the error code and error stream ocid
			// mappping to use

			JsonNode errorMMappingNodes = jsonNode.get("errormapping");

			for (int i = 0; i < errorMMappingNodes.size(); i++) {
				JsonNode node = errorMMappingNodes.get(i);
				String streamOCID = node.get("stream").asText();

				// check if the error stream OCIDs in the payload are correct
				if (!streamExist(streamOCID, streamAdminClient)) {
					LOGGER.log(Level.SEVERE,
							"Processing Failed.  Correct the errormapping section with correct stream OCID. {0}  doesnt exist",
							streamOCID);

					httpGatewayContext.setStatusCode(500);
					return streamOCID + " doesnt exist. Correct the errormapping section with correct stream OCID";

				}
				// store in a map
				errorStreamMapping.put(node.get("responsecode").asText(), streamOCID);

			}

			try {
				return processStreamMessages(streamOCIDToRetry, streamAdminClient, readPartition, readAfterOffset,
						errorStreamMapping, noOfMessagesToProcess);

			} catch (BmcException e) {
				LOGGER.severe(e.getLocalizedMessage());
				httpGatewayContext.setStatusCode(e.getStatusCode());
				return e.getLocalizedMessage();
			}

		} catch (JsonProcessingException jsonex) {
			LOGGER.severe(jsonex.getLocalizedMessage());
			httpGatewayContext.setStatusCode(500);
			return "Error occured in processing the payload " + jsonex.getLocalizedMessage();
		}

	}

	/**
	 * @param streamOCID
	 * @param streamAdminClient
	 * @return boolean Returns true if stream exist, else returns false.
	 * 
	 *         This method checks if a stream exist
	 */
	private boolean streamExist(String streamOCID, StreamAdminClient streamAdminClient) {

		ListStreamsRequest listRequest = ListStreamsRequest.builder().compartmentId(STREAM_COMPARTMENT_OCID)
				.id(streamOCID).lifecycleState(LifecycleState.Active).build();

		ListStreamsResponse listResponse = streamAdminClient.listStreams(listRequest);

		return !listResponse.getItems().isEmpty();

	}

	/**
	 * @param streamOCIDToRetry
	 * @param streamAdminClient
	 * @param readPartition
	 * @param readAfterOffset
	 * @param errorStreamMapping
	 * @param noOfMessagesToProcess
	 * @return String Returns the no. of processed and failed messages.
	 * 
	 *         This method gets the Stream from OCID, creates a Stream cursor and
	 *         then reads and processes individual messages. It returns the process
	 *         status, showing the no. of successfully processed messages, failed
	 *         messages and if end of stream is reached, returns endOfStream as
	 *         true.
	 */
	private String processStreamMessages(String streamOCIDToRetry, StreamAdminClient streamAdminClient,
			String readPartition, long readAfterOffset, Map<String, String> errorStreamMapping,
			int noOfMessagesToProcess) {

		// Get the Stream to retry
		Stream retryStream = getStream(streamOCIDToRetry, streamAdminClient);

		// Get the streamClient

		StreamClient retryStreamClient = StreamClient.builder().stream(retryStream).build(provider);

		// Get the cursor

		String cursor = getStreamCursor(retryStreamClient, readPartition, streamOCIDToRetry, readAfterOffset);

		// Read and process messages in stream using cursor

		return readMessagesFromStream(cursor, retryStreamClient, streamOCIDToRetry, errorStreamMapping,
				noOfMessagesToProcess, streamAdminClient);
	}

	/**
	 * @param streamOCID
	 * @param streamAdminClient
	 * @return Stream
	 * 
	 *         This method obtains the Stream object from the stream OCID.
	 */
	private Stream getStream(String streamOCID, StreamAdminClient streamAdminClient) {

		GetStreamResponse getResponse = streamAdminClient
				.getStream(GetStreamRequest.builder().streamId(streamOCID).build());

		return getResponse.getStream();
	}

	/**
	 * @param streamClient
	 * @param readPartition
	 * @param streamOCIDToRetry
	 * @param readAfterOffset
	 * @return String Returns a Stream cursor
	 * 
	 *         This method creates a Stream message cursor using an
	 *         AFTER_OFFSET/TRIM_HORIZON cursor type
	 */
	private String getStreamCursor(StreamClient streamClient, String readPartition, String streamOCIDToRetry,
			long readAfterOffset) {
		CreateCursorDetails cursorDetails = null;

		if (readAfterOffset == -1) {
			cursorDetails = CreateCursorDetails.builder().partition(readPartition).type(Type.TrimHorizon).build();

		} else {

			cursorDetails = CreateCursorDetails.builder().partition(readPartition).type(Type.AfterOffset)
					.offset(readAfterOffset).build();
		}

		CreateCursorRequest createCursorRequest = CreateCursorRequest.builder().streamId(streamOCIDToRetry)
				.createCursorDetails(cursorDetails).build();

		CreateCursorResponse cursorResponse = streamClient.createCursor(createCursorRequest);

		return cursorResponse.getCursor().getValue();
	}

	/**
	 * @param cursor
	 * @param streamClient
	 * @param streamOCIDToRetry
	 * @param errorStreamMapping
	 * @param readAfterOffset
	 * @param noOfMessagesToProcess
	 * @param StreamAdminClient
	 * @return String Returns the no. of processed and failed messages.
	 * 
	 *         This method is used to read the messages from stream
	 */
	private String readMessagesFromStream(String cursor, StreamClient streamClient, String streamOCIDToRetry,
			Map<String, String> errorStreamMapping, int noOfMessagesToProcess, StreamAdminClient streamAdminClient) {

		long lastReadOffset = 0;
		String endOfStreamMessage = "";
		GetMessagesRequest getRequest = GetMessagesRequest.builder().streamId(streamOCIDToRetry).cursor(cursor)
				.limit(noOfMessagesToProcess + 1).build();

		GetMessagesResponse getResponse = streamClient.getMessages(getRequest);
		List<Message> responseItems = getResponse.getItems();

		// if end of stream is reached, return

		if (responseItems.isEmpty()) {

			return "{\"endOfStream\": true}";
		}

		if (responseItems.size() < noOfMessagesToProcess) {

			endOfStreamMessage = ",\"endOfStream\": true";
		}

		// process the messages

		int successMessages = 0;
		int failedMessages = 0;
		String streamKey = "";
		String streamMessage = "";

		for (Message message : responseItems) {

			try {
				streamMessage = new String(message.getValue(), UTF_8);
				if (message.getKey() != null) {

					streamKey = new String(message.getKey(), UTF_8);
				} else {
					streamKey = "";
				}

				executeMessage(streamMessage, streamKey, errorStreamMapping, streamAdminClient);

				successMessages = successMessages + 1;

			} catch (Exception ex) {
				LOGGER.log(Level.SEVERE, "Retry Failed due to Exception in processing message. {0}",
						ex.getLocalizedMessage());
				ex.printStackTrace();

				populateErrorStream(streamMessage, streamKey, errorStreamMapping.get(String.valueOf("unexpectedError")),
						streamAdminClient);

				failedMessages = failedMessages + 1;

			}
			// Return the offset upto which messages were read
			lastReadOffset = message.getOffset();

			LOGGER.log(Level.INFO, "Read message at offset {0}", lastReadOffset);

		}

		return new StringBuilder("{\"lastReadOffset\":").append(lastReadOffset).append(" ,\"processedmessages\":")
				.append(successMessages).append(",\"failedMessages\":").append(failedMessages)
				.append(endOfStreamMessage).append("}").toString();

	}

	/**
	 * @param streamMessage
	 * @param streamKey
	 * @param errorStreamMapping
	 * @param StreamAdminClient
	 * @throws InterruptedException
	 * @throws IOException
	 * 
	 *                              This method parses target api payload and
	 *                              processes it.
	 * 
	 */
	private void executeMessage(String streamMessage, String streamKey, Map<String, String> errorStreamMapping,
			StreamAdminClient streamAdminClient) throws IOException, InterruptedException {

		HttpClient httpClient = HttpClient.newHttpClient();
		String targetRestApiPayload = "";
		String vaultSecretName = "";
		String targetRestApiOperation = "";
		String targetRestApi = "";
		StringBuilder failureMessage = new StringBuilder("");
		boolean targetApiCallFailed = false;

		HttpRequest request = null;
		int responseStatusCode;
		ObjectMapper objectMapper = new ObjectMapper();

		JsonNode jsonNode = objectMapper.readTree(streamMessage);

		// parse the stream message
		if (jsonNode.has("vaultSecretName")) {

			vaultSecretName = jsonNode.get("vaultSecretName").asText();
		}

		if (jsonNode.get("targetRestApi") != null) {
			targetRestApi = jsonNode.get("targetRestApi").asText();
		} else {
			targetApiCallFailed = true;
			failureMessage = new StringBuilder("targetRestApi node not found in payload. ");
		}
		if (jsonNode.has("targetRestApiOperation")) {

			targetRestApiOperation = jsonNode.get("targetRestApiOperation").asText();
			if (Arrays.stream(OPERATIONS).noneMatch(targetRestApiOperation::equals)) {
				targetApiCallFailed = true;
				failureMessage.append("targetRestApiOperation node doesnt contain PUT,POST or DELETE.");
			}

		} else {
			targetApiCallFailed = true;
			failureMessage.append("  targetRestApiOperation node is not found in payload.");
		}

		if (jsonNode.get("targetRestApiPayload") != null) {
			targetRestApiPayload = jsonNode.get("targetRestApiPayload").toString();
		}
		if (targetApiCallFailed) {
			LOGGER.log(Level.SEVERE, failureMessage.toString());
			populateErrorStream(streamMessage, streamKey, DEFAULT_ERROR_STREAM_OCID, streamAdminClient);
			return;

		}
		// Get the targetRestApiHeaders section of the json payload
		JsonNode headersNode = jsonNode.get("targetRestApiHeaders");
		Map<String, String> httpHeaders = new HashMap<>();

		for (int i = 0; i < headersNode.size(); i++) {
			JsonNode headerNode = headersNode.get(i);
			httpHeaders.put(headerNode.get("key").asText(), headerNode.get("value").asText());

		}

		switch (targetRestApiOperation) {

		case "PUT": {
			Builder builder = HttpRequest.newBuilder().PUT(HttpRequest.BodyPublishers.ofString(targetRestApiPayload))
					.uri(URI.create(targetRestApi));

			request = constructHttpRequest(builder, httpHeaders, vaultSecretName);
			break;

		}

		case "POST": {

			Builder builder = HttpRequest.newBuilder().POST(HttpRequest.BodyPublishers.ofString(targetRestApiPayload))
					.uri(URI.create(targetRestApi));

			request = constructHttpRequest(builder, httpHeaders, vaultSecretName);
			break;
		}

		case "DELETE": {
			Builder builder = HttpRequest.newBuilder().DELETE().uri(URI.create(targetRestApi));

			request = constructHttpRequest(builder, httpHeaders, vaultSecretName);
			break;
		}

		default:
			LOGGER.log(Level.SEVERE, "Target API not processed.");
		}
		HttpResponse<InputStream> response = null;

		response = httpClient.send(request, BodyHandlers.ofInputStream());

		responseStatusCode = response.statusCode();

		if ((Family.familyOf(responseStatusCode) == Family.SERVER_ERROR)
				|| (Family.familyOf(responseStatusCode) == Family.CLIENT_ERROR)) {

			if (errorStreamMapping.containsKey(String.valueOf(responseStatusCode))) {
				// move the message to an error stream if a stream corresponding to response
				// status is defined
				populateErrorStream(streamMessage, streamKey,
						errorStreamMapping.get(String.valueOf(responseStatusCode)), streamAdminClient);

			} else {
				// if there is no error stream defined for the REST response code, use the
				// default
				populateErrorStream(streamMessage, streamKey, DEFAULT_ERROR_STREAM_OCID, streamAdminClient);
			}
		}

	}

	/**
	 * @param builder
	 * @param httpHeaders
	 * @param vaultSecretName
	 * @return HttpRequest
	 * 
	 *         This method constructs http request to make the target REST API call
	 */
	private HttpRequest constructHttpRequest(Builder builder, Map<String, String> httpHeaders, String vaultSecretName) {

		if (!vaultSecretName.equals("")) {
			String authorizationHeaderName = "Authorization";
			// Read the Vault to get the auth token
			String authToken = getSecretFromVault(vaultSecretName);
			// add targetRestApiHeaders to the request
			// add authorization token to the request
			builder.header(authorizationHeaderName, authToken);
		}

		httpHeaders.forEach((k, v) -> builder.header(k, v));

		return builder.build();

	}

	/**
	 * @param vaultSecretName
	 * @return String Returns the token stored in Vault
	 * 
	 *         This method is used to get the auth token from the vault using
	 *         secretName
	 */
	private String getSecretFromVault(String vaultSecretName) {
		SecretsClient secretsClient = new SecretsClient(provider);

		GetSecretBundleByNameRequest getSecretBundleByNameRequest = GetSecretBundleByNameRequest.builder()

				.secretName(vaultSecretName).vaultId(VAULT_OCID).build();

		// get the secret
		GetSecretBundleByNameResponse getSecretBundleResponse = secretsClient
				.getSecretBundleByName(getSecretBundleByNameRequest);

		// get the bundle content details
		Base64SecretBundleContentDetails base64SecretBundleContentDetails = (Base64SecretBundleContentDetails) getSecretBundleResponse
				.getSecretBundle().getSecretBundleContent();
		secretsClient.close();

		return base64SecretBundleContentDetails.getContent();

	}

	/**
	 * @param streamMessage
	 * @param streamKey
	 * @param errorStreamOCID
	 * @param StreamAdminClient
	 * 
	 *                          This method is used to populate the error stream
	 *                          with the failed message
	 *
	 */
	private void populateErrorStream(String streamMessage, String streamKey, String errorStreamOCID,
			StreamAdminClient streamAdminClient) {

		// Construct the stream message

		PutMessagesDetails messagesDetails = PutMessagesDetails.builder().messages(Arrays.asList(
				PutMessagesDetailsEntry.builder().key(streamKey.getBytes()).value(streamMessage.getBytes()).build()))
				.build();

		PutMessagesRequest putRequest = PutMessagesRequest.builder().streamId(errorStreamOCID)
				.putMessagesDetails(messagesDetails).build();
		// Read the response

		PutMessagesResponse putResponse = StreamClient.builder().stream(getStream(errorStreamOCID, streamAdminClient))
				.build(provider).putMessages(putRequest);
		for (PutMessagesResultEntry entry : putResponse.getPutMessagesResult().getEntries()) {
			if (entry.getError() != null) {

				LOGGER.log(Level.SEVERE, String.format("Put message error  %s, in stream with OCID %s.",
						entry.getErrorMessage(), errorStreamOCID));

			} else {

				LOGGER.log(Level.INFO,
						String.format("Message pushed to offset %s, in partition  %s in stream with OCID %s",
								entry.getOffset(), entry.getPartition(), errorStreamOCID));

			}

		}

	}

}

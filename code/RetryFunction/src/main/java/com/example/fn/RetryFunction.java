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
import java.util.logging.Logger;

import javax.ws.rs.core.Response.Status.Family;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.oracle.bmc.streaming.requests.CreateCursorRequest;
import com.oracle.bmc.streaming.requests.GetMessagesRequest;
import com.oracle.bmc.streaming.requests.GetStreamRequest;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import com.oracle.bmc.streaming.responses.CreateCursorResponse;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import com.oracle.bmc.streaming.responses.GetStreamResponse;
import com.oracle.bmc.streaming.responses.PutMessagesResponse;

public class RetryFunction {
	private static final Logger LOGGER = Logger.getLogger(RetryFunction.class.getName());
	private final ResourcePrincipalAuthenticationDetailsProvider provider = ResourcePrincipalAuthenticationDetailsProvider
			.builder().build();
	private static final String VAULT_OCID = System.getenv().get("vault_ocid");
	private static final int NO_OF_MESSAGES_TO_PROCESS = 10;

	/**
	 * @param requestBody
	 * @return long
	 * 
	 *         This is the entry point of the Function call
	 */
	public String handleRequest(String requestBody)

	{

		Map<String, String> errorStreamMapping = new HashMap<>();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode jsonNode = null;
		try {
			jsonNode = mapper.readTree(requestBody);
		} catch (JsonProcessingException e) {
			LOGGER.severe("Retry Failed due to JsonProcessingException");
		}

		// Stream to retry

		String streamOCIDToRetry = jsonNode.path("streamOCIDToRetry").asText();
		// Stream Offset to start the reading

		long readOffset = jsonNode.path("readOffset").asLong();
		// Stream partition to read
		String readPartition = jsonNode.path("readPartition").asText();

		// Get the error mapping nodes to get the error code and error stream ocid
		// mappping to use

		JsonNode errorMMappingNodes = jsonNode.get("errormapping");

		for (int i = 0; i < errorMMappingNodes.size(); i++) {
			JsonNode node = errorMMappingNodes.get(i);
			errorStreamMapping.put(node.get("responsecode").asText(), node.get("stream").asText());

		}
		Stream retryStream = null;
		try {
			// Get the Stream to retry
			retryStream = getStream(streamOCIDToRetry);
		} catch (BmcException ex) {

			LOGGER.severe("Error occured in getting the RetryStream" + ex.getLocalizedMessage());
			return "Error occured in getting Retry Stream from streamOCIDToRetry " + streamOCIDToRetry;

		}

		StreamClient retryStreamClient = StreamClient.builder().stream(retryStream).build(provider);

		// Get the cursor

		String cursor = getStreamCursor(retryStreamClient, readPartition, streamOCIDToRetry, readOffset);

		// Read messages

		return readMessagesFromStream(cursor, retryStreamClient, streamOCIDToRetry, errorStreamMapping, readOffset);

	}

	/**
	 * @param streamOCID
	 * @return
	 * 
	 *         This method obtains the Stream object from the stream OCID.
	 */
	private Stream getStream(String streamOCID) {

		StreamAdminClient streamAdminClient = StreamAdminClient.builder().build(provider);
		GetStreamResponse getResponse = streamAdminClient
				.getStream(GetStreamRequest.builder().streamId(streamOCID).build());

		return getResponse.getStream();
	}

	/**
	 * @param streamClient
	 * @param readPartition
	 * @param streamOCIDToRetry
	 * @param readOffset
	 * @return String
	 * 
	 *         This method creates a Stream message cursor using an
	 *         AFTER_OFFSET/TRIM_HORIZON cursor type
	 */
	private String getStreamCursor(StreamClient streamClient, String readPartition, String streamOCIDToRetry,
			long readOffset) {
		CreateCursorDetails cursorDetails = null;

		if (readOffset == -1) {
			cursorDetails = CreateCursorDetails.builder().partition(readPartition).type(Type.TrimHorizon).build();

		} else {

			cursorDetails = CreateCursorDetails.builder().partition(readPartition).type(Type.AfterOffset)
					.offset(readOffset).build();
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
	 * @return String
	 * 
	 *         This method is used to read the messages from stream
	 */
	private String readMessagesFromStream(String cursor, StreamClient streamClient, String streamOCIDToRetry,
			Map<String, String> errorStreamMapping, long readOffset) {

		long latestOffset = 0;

		GetMessagesRequest getRequest = GetMessagesRequest.builder().streamId(streamOCIDToRetry).cursor(cursor)
				.limit(NO_OF_MESSAGES_TO_PROCESS).build();

		GetMessagesResponse getResponse = streamClient.getMessages(getRequest);
		List<Message> responseItems = getResponse.getItems();
		// if end of stream is reached, return

		if (responseItems.isEmpty()) {

			return "{	\"endOfStreamReached\": true,\"latestOffset\": " + readOffset + " ,\"processedmessages\":0 }";
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

				processMessage(streamMessage, streamKey, errorStreamMapping);

				successMessages = successMessages + 1;

				LOGGER.info("Processed message at offset" + latestOffset);
			} catch (Exception ex) {

				LOGGER.severe("Retry Failed due to Exception in processing message " + ex.getLocalizedMessage());
				populateErrorStream(streamMessage, streamKey,
						errorStreamMapping.get(String.valueOf("unexpectedError")));
				// Return the offset upto which messages were read
				failedMessages = failedMessages + 1;

			}
			latestOffset = message.getOffset();

		}

		return "{	\"endOfStream\": false,\"latestOffset\": " + latestOffset + " ,\"processedmessages\":"
				+ successMessages + " ,\"failedMessages\":" + failedMessages + "}";

	}

	/**
	 * @param streamMessage
	 * @param streamKey
	 * @param errorStreamMapping This method parses the incoming message and
	 *                           processes it.
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void processMessage(String streamMessage, String streamKey, Map<String, String> errorStreamMapping)
			throws Exception {

		String targetRestApiPayload = null;
		HttpClient httpClient = HttpClient.newHttpClient();

		HttpRequest request = null;
		int responseStatusCode;
		ObjectMapper objectMapper = new ObjectMapper();

		JsonNode jsonNode = objectMapper.readTree(streamMessage);
		System.out.print("jsonNode" + jsonNode);

		// parse the stream message
		String targetRestApi = jsonNode.get("targetRestApi").asText();
		String targetRestApiOperation = jsonNode.get("targetRestApiOperation").asText();
		String vaultSecretName = jsonNode.get("vaultSecretName").asText();

		targetRestApiPayload = jsonNode.get("targetRestApiPayload").toString();
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
			// add targetRestApiHeaders to the request
			request = constructHttpRequest(builder, httpHeaders, vaultSecretName);
			break;
		}

		default:

			throw new Exception("Unhandled operation in targetRestApiOperation node in the message ");
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
						errorStreamMapping.get(String.valueOf(responseStatusCode)));

			} else {
				// if there is no error stream defined for the REST response code, use the
				// default
				populateErrorStream(streamMessage, streamKey, errorStreamMapping.get(String.valueOf("unmapped")));
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

		String authorizationHeaderName = "Authorization";
		// Read the Vault to get the auth token
		String authToken = getSecretFromVault(vaultSecretName);
		// add targetRestApiHeaders to the request

		httpHeaders.forEach((k, v) -> builder.header(k, v));
		// add authorization token to the request
		builder.header(authorizationHeaderName, authToken);
		HttpRequest request = builder.build();
		return request;
	}

	/**
	 * @param vaultSecretName
	 * @return String
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

		String secret = base64SecretBundleContentDetails.getContent();

		return secret;

	}

	/**
	 * @param streamMessage
	 * @param streamKey
	 * @param errorStreamOCID
	 * 
	 *                        This method is used to populate the error stream with
	 *                        the failed message
	 *
	 */
	private void populateErrorStream(String streamMessage, String streamKey, String errorStreamOCID) {

		// Construct the stream message

		PutMessagesDetails messagesDetails = PutMessagesDetails.builder().messages(Arrays.asList(
				PutMessagesDetailsEntry.builder().key(streamKey.getBytes()).value(streamMessage.getBytes()).build()))
				.build();

		PutMessagesRequest putRequest = PutMessagesRequest.builder().streamId(errorStreamOCID)
				.putMessagesDetails(messagesDetails).build();
		// Read the response

		PutMessagesResponse putResponse = StreamClient.builder().stream(getStream(errorStreamOCID)).build(provider)
				.putMessages(putRequest);
		for (PutMessagesResultEntry entry : putResponse.getPutMessagesResult().getEntries()) {
			if (entry.getError() != null) {

				LOGGER.severe("Put message error " + entry.getErrorMessage());
			} else {

				LOGGER.info("Message pushed to offset " + entry.getOffset() + " in partition " + entry.getPartition());
			}

		}

	}

}

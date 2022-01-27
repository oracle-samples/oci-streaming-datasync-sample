//Copyright (c)  2021,  Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

//This Function gets the messages from  the DataSyncStream   calls the target application’s API.
//If there is a failure in target application API call, the messages are sent to Error Streams. 
//The Error Streams to use, are configurable at the Function Application level. 

package com.example.fn;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.ws.rs.core.Response.Status.Family;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.secrets.SecretsClient;
import com.oracle.bmc.secrets.model.Base64SecretBundleContentDetails;
import com.oracle.bmc.secrets.requests.GetSecretBundleByNameRequest;
import com.oracle.bmc.secrets.responses.GetSecretBundleByNameResponse;
import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.PutMessagesDetails;
import com.oracle.bmc.streaming.model.PutMessagesDetailsEntry;
import com.oracle.bmc.streaming.model.PutMessagesResultEntry;
import com.oracle.bmc.streaming.model.Stream;
import com.oracle.bmc.streaming.requests.GetStreamRequest;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import com.oracle.bmc.streaming.responses.GetStreamResponse;
import com.oracle.bmc.streaming.responses.PutMessagesResponse;

public class ReadDataStreamFunction {

	private static final Logger LOGGER = Logger.getLogger(ReadDataStreamFunction.class.getName());
	private final ResourcePrincipalAuthenticationDetailsProvider provider = ResourcePrincipalAuthenticationDetailsProvider
			.builder().build();
	private static final String VAULT_OCID = System.getenv().get("vault_ocid");
	private static final String UNRECOVERABLE_ERROR_STREAM_OCID = System.getenv()
			.get("unrecoverable_error_stream_ocid");
	private static final String SERVICEUNAVAILABLE_ERROR_STREAM_OCID = System.getenv()
			.get("serviceUnavailable_error_stream_ocid");
	private static final String INTERNALSERVER_ERROR_STREAM_OCID = System.getenv()
			.get("internalserver_error_stream_ocid");
	private static final String DEFAULT_ERROR_STREAM_OCID = System.getenv().get("default_error_stream_ocid");

	public ReadDataStreamFunction() {

	}

	/**
	 * @param incomingMessage
	 * 
	 *                        This is the entry point of the function execution.
	 */
	public void handleRequest(String incomingMessage) {

		ObjectMapper objectMapper = new ObjectMapper();
		// Read the stream messages

		JsonNode jsonTree = null;
		try {
			jsonTree = objectMapper.readTree(incomingMessage);
		} catch (JsonProcessingException e) {
			LOGGER.severe("Message processing failed with JSONProcessing exception");
		}

		for (int i = 0; i < jsonTree.size(); i++) {
			JsonNode jsonNode = jsonTree.get(i);
			// Get the stream key and value

			String streamKey = jsonNode.get("key").asText();
			String streamMessage = jsonNode.get("value").asText();
			// Decode the stream message

			String decodedMessageValue = new String(Base64.getDecoder().decode(streamMessage.getBytes()));

			try {

				processMessage(decodedMessageValue, streamKey);

			} catch (Exception ex) {

				LOGGER.severe("Message failed with exception " + ex.getLocalizedMessage());

				populateErrorStream(streamMessage, streamKey, UNRECOVERABLE_ERROR_STREAM_OCID);
			}

		}

	}

	/**
	 * @param streamMessage
	 * @param streamKey     This method parses the incoming message and processes it
	 *                      based on the targetRestApiOperation defined in the
	 *                      message
	 */
	private void processMessage(String streamMessage, String streamKey) throws Exception {
		HttpClient httpClient = HttpClient.newHttpClient();

		String targetRestApiPayload = "";

		int responseStatusCode = 0;
		ObjectMapper objectMapper = new ObjectMapper();
		HttpRequest request = null;
		JsonNode jsonNode = null;

		jsonNode = objectMapper.readTree(streamMessage);
		// parse the incoming message

		String vaultSecretName = jsonNode.get("vaultSecretName").asText();
		String targetRestApi = jsonNode.get("targetRestApi").asText();
		String targetRestApiOperation = jsonNode.get("targetRestApiOperation").asText();

		if (jsonNode.get("targetRestApiPayload") != null) {
			targetRestApiPayload = jsonNode.get("targetRestApiPayload").toString();
		}
		// Get the targetRestApiHeaders section of the json payload
		JsonNode headersNode = jsonNode.get("targetRestApiHeaders");
		Map<String, String> httpHeaders = new HashMap<>();

		for (int i = 0; i < headersNode.size(); i++) {
			JsonNode headerNode = headersNode.get(i);
			httpHeaders.put(headerNode.get("key").asText(), headerNode.get("value").asText());

		}
		// process the messages based on the operation
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
		}
		default:

			throw new Exception("Unhandled operation in targetRestApiOperation node in the message ");
		}

		// make the http request call

		HttpResponse<InputStream> response = httpClient.send(request, BodyHandlers.ofInputStream());
		// get the status code
		responseStatusCode = response.statusCode();

		// Populate error streams in case of a failure
		String errorStreamOCID = "";

		if ((Family.familyOf(responseStatusCode) == Family.SERVER_ERROR)
				|| (Family.familyOf(responseStatusCode) == Family.CLIENT_ERROR)) {

			switch (responseStatusCode) {

			case 503: {
				errorStreamOCID = SERVICEUNAVAILABLE_ERROR_STREAM_OCID;
				break;
			}
			case 500: {
				errorStreamOCID = INTERNALSERVER_ERROR_STREAM_OCID;
				break;
			}

			case 400: {
				errorStreamOCID = UNRECOVERABLE_ERROR_STREAM_OCID;
				break;
			}

			default:
				errorStreamOCID = DEFAULT_ERROR_STREAM_OCID;

			}

			populateErrorStream(streamMessage, streamKey, errorStreamOCID);
		}

	}

	/**
	 * @param builder
	 * @param httpHeaders
	 * @param vaultSecretName
	 * @return HttpRequest
	 * 
	 *         This method constructs http request for the target application call
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
	 * @param streamOCID
	 * @return Stream
	 * 
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
	 * @param streamMessage
	 * @param streamKey
	 * @param errorStreamOCID
	 * 
	 *                        This method is used to populate the error stream with
	 *                        the failed message
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

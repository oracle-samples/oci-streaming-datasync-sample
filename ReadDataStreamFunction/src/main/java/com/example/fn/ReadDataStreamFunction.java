//Copyright (c)  2021,  Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
	
	
package com.example.fn;

import java.io.IOException;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.secrets.SecretsClient;
import com.oracle.bmc.secrets.model.Base64SecretBundleContentDetails;
import com.oracle.bmc.secrets.requests.GetSecretBundleRequest;
import com.oracle.bmc.secrets.responses.GetSecretBundleResponse;
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

	private HttpClient httpClient = null;

	private SecretsClient secretsClient = null;
	private StreamAdminClient streamAdminClient = null;

	
	

	public ReadDataStreamFunction() {
		streamAdminClient = StreamAdminClient.builder().build(provider);

		httpClient = HttpClient.newHttpClient();
		secretsClient = new SecretsClient(provider);

	}

	/**
	 * @param incomingMessage
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 * This is the entry point of the function
	 *  execution.
	 */
	public void handleRequest(String incomingMessage) throws IOException, InterruptedException {

		ObjectMapper objectMapper = new ObjectMapper();
		//Read the stream messages
		

		JsonNode jsonTree = objectMapper.readTree(incomingMessage);
        
		for (int i = 0; i < jsonTree.size(); i++) {
			JsonNode jsonNode = jsonTree.get(i);
			//Get the stream key and value

			String streamKey = jsonNode.get("key").asText();
			String streamMessage = jsonNode.get("value").asText();
			//Decode the stream message

			String decodedMessageValue = new String(Base64.getDecoder().decode(streamMessage.getBytes()));
			

			processMessage(decodedMessageValue, streamKey);

		}

	}

	/**
	 * @param streamMessage
	 * @param streamKey
	 * @throws IOException
	 * @throws InterruptedException 
	 * This method parses the incoming message and
	 * processes it based on the targetRestApiOperation defined in
	 *  the message
	 */
	private void processMessage(String streamMessage, String streamKey) throws IOException, InterruptedException {

		String targetRestApiPayload = "";

		int responseStatusCode = 0;
		ObjectMapper objectMapper = new ObjectMapper();
		HttpRequest request = null;
		JsonNode jsonNode = objectMapper.readTree(streamMessage);
		// parse the message and get the vault secret OCID where the authorization header token is stored
		//REST API targetRestApi  of the target application and the REST targetRestApiOperation
		String vaultSecretId = jsonNode.get("vaultSecretId").asText();
		String targetRestApi = jsonNode.get("targetRestApi").asText();
		String targetRestApiOperation = jsonNode.get("targetRestApiOperation").asText();
        //Get the json payload section
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

		switch (targetRestApiOperation) {

		case "PUT": {
			Builder builder = HttpRequest.newBuilder().PUT(HttpRequest.BodyPublishers.ofString(targetRestApiPayload))
					.uri(URI.create(targetRestApi));

			request = constructHttpRequest(builder, httpHeaders, vaultSecretId);
			break;

		}

		case "POST": {

			Builder builder = HttpRequest.newBuilder().POST(HttpRequest.BodyPublishers.ofString(targetRestApiPayload))
					.uri(URI.create(targetRestApi));

			request = constructHttpRequest(builder, httpHeaders, vaultSecretId);
			break;
		}

		case "DELETE": {
			Builder builder = HttpRequest.newBuilder().DELETE().uri(URI.create(targetRestApi));

			request = constructHttpRequest(builder, httpHeaders, vaultSecretId);
		}
		}

		// make the http request call

		HttpResponse<InputStream> response = httpClient.send(request, BodyHandlers.ofInputStream());
		// get the status code
		responseStatusCode = response.statusCode();
		
		// Get the error stream OCID mapped to the REST response error code

		String errorStreamOCID = System.getenv().get("_" + responseStatusCode + "_error_stream_ocid");
		
		if (errorStreamOCID != null) {
			Stream errorStream = getStream(errorStreamOCID);
			// move the message to an error stream if a stream corresponding to response
			// status is defined
			populateErrorStream(streamMessage, streamKey, errorStream, errorStreamOCID);
			LOGGER.info("Inside populate error stream" + errorStreamOCID);
		} else {
			String defaultErrorStreamOCID = System.getenv().get("default_error_stream_ocid");
			Stream errorStream = getStream(defaultErrorStreamOCID);
			populateErrorStream(streamMessage, streamKey, errorStream, defaultErrorStreamOCID);
			LOGGER.info("Inside populate default error stream" + defaultErrorStreamOCID);

		}

	}

	/**
	 * @param builder
	 * @param httpHeaders
	 * @param vaultSecretId
	 * @return HttpRequest
	 * 
	 *  This method constructs http request for the target application call
	 */
	private HttpRequest constructHttpRequest(Builder builder, Map<String, String> httpHeaders, String vaultSecretId) {

		String authorizationHeaderName = "Authorization";
		// Read the Vault to get the auth token
		String authToken = getSecretFromVault(vaultSecretId);
		// add targetRestApiHeaders to the request

		httpHeaders.forEach((k, v) -> builder.header(k, v));
		// add authorization token to the request
		builder.header(authorizationHeaderName, authToken);
		HttpRequest request = builder.build();
		return request;
	}

	/**
	 * @param vaultSecretId
	 * @return String
	 * 
	 * This method is used to get the auth token from the vault. The secret
	 * OCID is present in the message as the vaultSecretId and it is used for
	 * getting the secret content
	 */
	private String getSecretFromVault(String vaultSecretId) {

		GetSecretBundleRequest getSecretBundleRequest = GetSecretBundleRequest.builder()

				.secretId(vaultSecretId).stage(GetSecretBundleRequest.Stage.Current).build();

		// get the secret
		GetSecretBundleResponse getSecretBundleResponse = secretsClient.getSecretBundle(getSecretBundleRequest);

		// get the bundle content details
		Base64SecretBundleContentDetails base64SecretBundleContentDetails = (Base64SecretBundleContentDetails) getSecretBundleResponse
				.getSecretBundle().getSecretBundleContent();

		String secret = base64SecretBundleContentDetails.getContent();

		return secret;

	}

	/**
	 * @param streamOCID
	 * @return Stream This method obtains the Stream object from the stream OCID.
	 */
	private Stream getStream(String streamOCID) {
		GetStreamResponse getResponse = streamAdminClient
				.getStream(GetStreamRequest.builder().streamId(streamOCID).build());
		return getResponse.getStream();
	}

	/**
	 * @param streamMessage
	 * @param streamKey
	 * @param errorStream
	 * @param errorStreamOCID
	 * 
	 * This method is used to populate the error stream with
	 * the failed message
	 */
	private void populateErrorStream(String streamMessage, String streamKey, Stream errorStream,
			String errorStreamOCID) {
		// Construct the stream message

		PutMessagesDetails messagesDetails = PutMessagesDetails.builder().messages(Arrays.asList(
				PutMessagesDetailsEntry.builder().key(streamKey.getBytes()).value(streamMessage.getBytes()).build()))
				.build();

		PutMessagesRequest putRequest = PutMessagesRequest.builder().streamId(errorStreamOCID)
				.putMessagesDetails(messagesDetails).build();

		// Read the response

		PutMessagesResponse putResponse = StreamClient.builder().stream(errorStream).build(provider)
				.putMessages(putRequest);
		for (PutMessagesResultEntry entry : putResponse.getPutMessagesResult().getEntries()) {
			if (entry.getError() != null) {

				LOGGER.info("Put message error " + entry.getErrorMessage());
			} else {

				LOGGER.info("Message pushed to offset " + entry.getOffset() + " in partition " + entry.getPartition());
			}

		}

	}

}

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
import java.util.Map;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.secrets.SecretsClient;
import com.oracle.bmc.secrets.model.Base64SecretBundleContentDetails;
import com.oracle.bmc.secrets.requests.GetSecretBundleRequest;
import com.oracle.bmc.secrets.responses.GetSecretBundleResponse;
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
	private HttpClient httpClient = null;

	private StreamClient streamClient = null;

	private String streamOCIDToRetry, readPartition = "";
	private long readOffset;

	private final ResourcePrincipalAuthenticationDetailsProvider provider = ResourcePrincipalAuthenticationDetailsProvider
			.builder().build();
	private StreamAdminClient streamAdminClient = null;
	private Map<String, String> errorStreamMapping = new HashMap<>();
	private SecretsClient secretsClient = null;

	public RetryFunction() {

		streamAdminClient = StreamAdminClient.builder().build(provider);
		httpClient = HttpClient.newHttpClient();
		secretsClient = new SecretsClient(provider);

	}

	/**
	 * @param requestBody
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public long handleRequest(String requestBody) throws IOException, InterruptedException

	{

		parseRequestBody(requestBody);

		Stream retryStream = getStream(streamOCIDToRetry);

		streamClient = StreamClient.builder().stream(retryStream).build(provider);

		String cursor = getSourceStreamCursor();

		return readMessagesFromSourceStream(cursor);

	}

	/**
	 * @param requestBody
	 * @throws JsonMappingException
	 * @throws JsonProcessingException
	 * 
	 */
	private void parseRequestBody(String requestBody) throws JsonProcessingException {

		ObjectMapper mapper = new ObjectMapper();
		JsonNode jsonNode = mapper.readTree(requestBody);

		streamOCIDToRetry = jsonNode.path("streamOCIDToRetry").asText();
		LOGGER.info("streamOCIDToRetry"+streamOCIDToRetry);

		readOffset = jsonNode.path("readOffset").asLong();
		readPartition = jsonNode.path("readPartition").asText();

		JsonNode errorMMappingNodes = jsonNode.get("errormapping");

		for (int i = 0; i < errorMMappingNodes.size(); i++) {
			JsonNode node = errorMMappingNodes.get(i);
			errorStreamMapping.put(node.get("responsecode").asText(), node.get("stream").asText());

		}

	}

	/**
	 * @param streamOCID
	 * @return
	 * 
	 *         This method obtains the Stream object from the stream OCID.
	 */
	private Stream getStream(String streamOCID) {
		GetStreamResponse getResponse = streamAdminClient
				.getStream(GetStreamRequest.builder().streamId(streamOCID).build());
		return getResponse.getStream();
	}

	/**
	 * @return String
	 * 
	 *         This method creates a message cursor using an offset cursor type
	 */
	private String getSourceStreamCursor() {

		CreateCursorDetails cursorDetails = CreateCursorDetails.builder().partition(readPartition).type(Type.AtOffset)
				.offset(readOffset).build();

		CreateCursorRequest createCursorRequest = CreateCursorRequest.builder().streamId(streamOCIDToRetry)
				.createCursorDetails(cursorDetails).build();

		CreateCursorResponse cursorResponse = streamClient.createCursor(createCursorRequest);

		return cursorResponse.getCursor().getValue();
	}

	/**
	 * @param cursor
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 *                              This method is used to read the messages from
	 *                              the source stream based on the offset
	 */
	private long readMessagesFromSourceStream(String cursor) throws IOException, InterruptedException {

		long latestOffset = 0;

		// By default, the service returns as many messages as possible. You can use the
		// limit parameter
		// to specify any value up to 10,000, but consider your average message size to
		// avoid exceeding throughput on the stream.
		GetMessagesRequest getRequest = GetMessagesRequest.builder().streamId(streamOCIDToRetry).cursor(cursor)
				.limit(10).build();

		GetMessagesResponse getResponse = streamClient.getMessages(getRequest);

		// process the messages

		for (Message message : getResponse.getItems()) {

			if (message.getKey() != null) {
				processMessage(new String(message.getValue(), UTF_8), new String(message.getKey(), UTF_8));
			} else {
				processMessage(new String(message.getValue(), UTF_8), "");
			}

			latestOffset = message.getOffset();

		}

		return latestOffset;

	}

	/**
	 * @param streamMessage
	 * @param streamKey
	 * @throws IOException
	 * @throws InterruptedException
	 * 
	 *                              This method parses the incoming message and
	 *                              processes it based on the operation defined in
	 *                              the message
	 */

	private void processMessage(String streamMessage, String streamKey) throws IOException, InterruptedException {

		String data = null;

		HttpRequest request = null;
		int responseStatusCode;
		ObjectMapper objectMapper = new ObjectMapper();

		JsonNode jsonNode = objectMapper.readTree(streamMessage);
		// parse the streammessage section of the json payload
		String url = jsonNode.get("url").asText();
		String operation = jsonNode.get("operation").asText();
		String vaultSecretId = jsonNode.get("vaultSecretId").asText();
		data = jsonNode.get("data").toString();
		// Get the headers section of the json payload
		JsonNode headersNode = jsonNode.get("headers");
		Map<String, String> httpHeaders = new HashMap<>();

		for (int i = 0; i < headersNode.size(); i++) {
			JsonNode headerNode = headersNode.get(i);
			httpHeaders.put(headerNode.get("key").asText(), headerNode.get("value").asText());

		}

		switch (operation) {

		case "PUT": {
			Builder builder = HttpRequest.newBuilder().PUT(HttpRequest.BodyPublishers.ofString(data))
					.uri(URI.create(url));

			request = constructHttpRequest(builder, httpHeaders, vaultSecretId);
			break;

		}

		case "POST": {

			Builder builder = HttpRequest.newBuilder().POST(HttpRequest.BodyPublishers.ofString(data))
					.uri(URI.create(url));

			request = constructHttpRequest(builder, httpHeaders, vaultSecretId);
			break;
		}

		case "DELETE": {
			Builder builder = HttpRequest.newBuilder().DELETE().uri(URI.create(url));
			// add headers to the request
			request = constructHttpRequest(builder, httpHeaders, vaultSecretId);
		}
		}

		HttpResponse<InputStream> response = null;

		response = httpClient.send(request, BodyHandlers.ofInputStream());

		responseStatusCode = response.statusCode();
		// Get the error stream OCID mapped to the REST response error code

		if (errorStreamMapping.containsKey(String.valueOf(responseStatusCode))) {
			// move the message to an error stream if a stream corresponding to response
			// status is defined
			populateErrorStream(streamMessage, streamKey, errorStreamMapping.get(String.valueOf(responseStatusCode)));

		} else {
			populateErrorStream(streamMessage, streamKey, errorStreamMapping.get(String.valueOf("unmapped")));
		}

	}

	/**
	 * @param builder
	 * @param httpHeaders
	 * @param vaultSecretId
	 * @return HttpRequest
	 * 
	 *         This method constructs http request
	 */
	private HttpRequest constructHttpRequest(Builder builder, Map<String, String> httpHeaders, String vaultSecretId) {

		String authorizationHeaderName = "Authorization";
		// Read the Vault to get the auth token
		String authToken = getSecretFromVault(vaultSecretId);
		// add headers to the request

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
	 *         This method is used to get the auth token from the vault. The secret
	 *         OCID is present in the message as the vaultSecretId and it is used for
	 *         getting the secret content
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

				LOGGER.info("Put message error " + entry.getErrorMessage());
			} else {

				LOGGER.info("Message pushed to offset " + entry.getOffset() + " in partition " + entry.getPartition());
			}

		}

	}

}

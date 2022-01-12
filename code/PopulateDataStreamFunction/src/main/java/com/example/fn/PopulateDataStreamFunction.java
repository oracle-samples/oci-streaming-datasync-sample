//Copyright (c)  2021,  Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
package com.example.fn;

import java.util.Arrays;
import java.util.Optional;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fnproject.fn.api.Headers;
import com.fnproject.fn.api.QueryParameters;
import com.fnproject.fn.api.httpgateway.HTTPGatewayContext;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
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
import com.oracle.bmc.vault.VaultsClient;
import com.oracle.bmc.vault.model.Base64SecretContentDetails;
import com.oracle.bmc.vault.model.CreateSecretDetails;
import com.oracle.bmc.vault.model.SecretContentDetails;
import com.oracle.bmc.vault.requests.CreateSecretRequest;
import com.oracle.bmc.vault.responses.CreateSecretResponse;

public class PopulateDataStreamFunction {
	private static final Logger LOGGER = Logger.getLogger(PopulateDataStreamFunction.class.getName());
	private final ResourcePrincipalAuthenticationDetailsProvider provider = ResourcePrincipalAuthenticationDetailsProvider
			.builder().build();
	private static final String VAULT_OCID = System.getenv().get("vault_ocid");
	private static final String VAULT_COMPARTMENT_OCID = System.getenv().get("vault_compartment_ocid");
	private static final String VAULT_KEY_OCID = System.getenv().get("vault_key_ocid");
	private StreamAdminClient streamAdminClient = null;
	private StreamClient streamClient = null;
	private VaultsClient vaultClient = null;	
	private String streamKey, streamMessage, vaultSecretId = "";
	

	public PopulateDataStreamFunction() {

		streamAdminClient = StreamAdminClient.builder().build(provider);

		vaultClient = new VaultsClient(provider);

	}

	/**
	 * @param httpGatewayContext
	 * @param requestBody
	 * @throws JsonMappingException
	 * @throws JsonProcessingException
	 * 
	 * This is the entry point of the function
	 *  execution.
	 */
	public void handleRequest(HTTPGatewayContext httpGatewayContext, String requestBody)
			throws JsonMappingException, JsonProcessingException {

		
		Stream stream = null;

		QueryParameters queryparams = httpGatewayContext.getQueryParameters();
		// Read the request header to get the authorization header value.
		// This will be stored in a vault
		Headers headers = httpGatewayContext.getHeaders();
		String authorizarionHeader = headers.get("Authorization").get();
		// This is the OCID of the stream to which data is populated.
		
		Optional<String> streamOCID = queryparams.get("streamOCID");
		String streamOCIDValue = "";
		if (streamOCID.isPresent()) {
			streamOCIDValue = streamOCID.get();
		}

		// parse the request body to get the message's key and value.
		parseRequestBody(requestBody);
		// Store the authorization header in a vault
		String secretOcid = createSecretInVault(authorizarionHeader);

		// Every message will have a vaultSecretId to use as the name of the secret. vaultSecretId value in the message
		// is replaced by the secret's OCID once the secret is created in Vault. This secret OCID is later used
		//by other functions to get the secret content while processing the message.

		String messageUpdatedWithSecretOCID = streamMessage.replaceAll(vaultSecretId, secretOcid);
        //Get the Stream 
		stream = getStream(streamOCIDValue);
		streamClient = StreamClient.builder().stream(stream).build(provider);

		//store message in Stream

		storeMessageinStream(messageUpdatedWithSecretOCID,  streamOCIDValue) ;

	}

	/**
	 * @param requestBody
	 * @throws JsonMappingException
	 * @throws JsonProcessingException
	 * 
	 * This method parses the request body and gets
	 * the message key, the message value. It also
	 * obtains the unique id of the message from the
	 *  message value
	 */
	private void parseRequestBody(String requestBody) throws JsonProcessingException {

		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode jsonNode = objectMapper.readTree(requestBody);

		// Get the message key and the actual content to be stored in the stream.
		// streamKey will be used as the stream message's key

		streamKey = jsonNode.path("streamKey").asText();

		streamMessage = jsonNode.path("streamMessage").toString();
		// To get the vaultSecretId from streamMessage
		JsonNode streamMessageNode = objectMapper.readTree(streamMessage);

		vaultSecretId = streamMessageNode.get("vaultSecretId").asText();

	}

	/**
	 * @param streamOCID
	 * @return Stream 
	 * This method obtains the Stream object from the stream OCID.
	 */
	private Stream getStream(String streamOCID) {
		GetStreamResponse getResponse = streamAdminClient
				.getStream(GetStreamRequest.builder().streamId(streamOCID).build());
		return getResponse.getStream();
	}

	/**
	 * @param authorizationHeader
	 * @return String
	 * 
	 * This method is to store the auth token in a vault. It generates a
	 * secret with content as auth token and name as the vaultSecretId.
	 * The secret is stored in the vault in the compartment specified in
	 * application configuration variables. The secret encryption key used
	 * is also specified in the application configuration variable. After
	 * the creation of the secret, the OCID of the newly created secret is
	 * returned. This OCID will replace the vaultSecretId of the message and
	 * later will be used for reading the secret content  by other
	 * functions.
	 */
	private String createSecretInVault(String authorizationHeader) {

//Create a new secret with content as the authorization header value and name as vaultSecretId
		Base64SecretContentDetails base64SecretContentDetails = Base64SecretContentDetails.builder()
				.content(authorizationHeader).name("secretcontent" + vaultSecretId)
				.stage(SecretContentDetails.Stage.Current).build();
		
		// The secret is created in the compartment and vault specified in application
		// configuration variable
		// The secret uses the key mentioned in the application configuration variable
		CreateSecretDetails createSecretDetails = CreateSecretDetails.builder().compartmentId(VAULT_COMPARTMENT_OCID)
				.secretName(vaultSecretId).keyId(VAULT_KEY_OCID).vaultId(VAULT_OCID)
				.secretContent(base64SecretContentDetails).build();
		CreateSecretRequest createSecretRequest = CreateSecretRequest.builder().createSecretDetails(createSecretDetails)
				.build();
		CreateSecretResponse createSecretResponse = vaultClient.createSecret(createSecretRequest);

		// After the secret is created get its OCID as this value is needed by other
		// functions to read the secret
		return createSecretResponse.getSecret().getId();

	}
	/**
	 * @param message
	 * @param streamOCID
	 * This method stores the message in the Stream
	 */
	private void storeMessageinStream(String message, String streamOCID) {
		String result = null;

	PutMessagesDetails messagesDetails = PutMessagesDetails.builder().messages(Arrays.asList(PutMessagesDetailsEntry
			.builder().key(streamKey.getBytes()).value(message.getBytes()).build())).build();
	PutMessagesRequest putRequest = PutMessagesRequest.builder().streamId(streamOCID)
			.putMessagesDetails(messagesDetails).build();
	PutMessagesResponse putResponse = streamClient.putMessages(putRequest);
	for (PutMessagesResultEntry entry : putResponse.getPutMessagesResult().getEntries()) {
		if (entry.getError() != null) {
			result = "Put message error " + entry.getErrorMessage();
			LOGGER.info(result);
		} else {
			result = "Message pushed to offset " + entry.getOffset() + " in partition " + entry.getPartition();
			LOGGER.info(result);
		}
	}
	for (PutMessagesResultEntry entry : putResponse.getPutMessagesResult().getEntries()) {
		if (StringUtils.isNotBlank(entry.getError())) {
			LOGGER.info(String.format("Error: ", entry.getError(), entry.getErrorMessage()));
		} else {
			LOGGER.info(String.format("Published message to partition , offset .", entry.getPartition(),
					entry.getOffset()));
		}
	}
	}
}

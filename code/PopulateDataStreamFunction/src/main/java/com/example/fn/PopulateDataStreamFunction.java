// Copyright (c)  2022,  Oracle and/or its affiliates.
// Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

// This Function is used to populate the DataSyncStream . 
// It is invoked when the Source Application/s post data to the REST API exposed using API Gateway.
package com.example.fn;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fnproject.fn.api.Headers;
import com.fnproject.fn.api.httpgateway.HTTPGatewayContext;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.model.BmcException;
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
import com.oracle.bmc.vault.model.SecretSummary;
import com.oracle.bmc.vault.requests.CreateSecretRequest;
import com.oracle.bmc.vault.requests.ListSecretsRequest;
import com.oracle.bmc.vault.responses.ListSecretsResponse;

public class PopulateDataStreamFunction {
	private static final Logger LOGGER = Logger.getLogger(PopulateDataStreamFunction.class.getName());
	private final ResourcePrincipalAuthenticationDetailsProvider provider = ResourcePrincipalAuthenticationDetailsProvider
			.builder().build();
	private static final String VAULT_OCID = System.getenv().get("vault_ocid");
	private static final String VAULT_COMPARTMENT_OCID = System.getenv().get("vault_compartment_ocid");
	private static final String VAULT_KEY_OCID = System.getenv().get("vault_key_ocid");
	private static final String DATA_STREAM_OCID = System.getenv().get("data_stream_ocid");

	/**
	 * @param httpGatewayContext
	 * @param requestBody
	 * @return String
	 * @throws JsonMappingException
	 * @throws JsonProcessingException
	 * 
	 *                                 This is the entry point of the function
	 *                                 execution.
	 */
	public String handleRequest(HTTPGatewayContext httpGatewayContext, String requestBody) {

		String streamKey = "";
		String streamMessage = "";
		String vaultSecretName = "";

		// Read the request header to get the authorization header value.
		// This will be stored in a vault
		Headers headers = httpGatewayContext.getHeaders();
		Optional<String> authorizationHeaderOpt = headers.get("Authorization");

		ObjectMapper objectMapper = new ObjectMapper();
		try {
			JsonNode jsonNode = objectMapper.readTree(requestBody);

			// Get the message key and the actual content to be stored in the stream.
			// streamKey will be used as the stream message's key

			streamKey = jsonNode.path("streamKey").asText();

			streamMessage = jsonNode.path("streamMessage").toString();
			// To get the vaultSecretName from streamMessage
			JsonNode streamMessageNode = objectMapper.readTree(streamMessage);
			if (authorizationHeaderOpt.isPresent()) {

				String authorizationHeader = authorizationHeaderOpt.get();
				vaultSecretName = streamMessageNode.get("vaultSecretName").asText();

				// If secret with the name vaultSecretName is not already present,
				// create a secret
				if (checkSecretInVault(vaultSecretName)) {
					createSecretInVault(authorizationHeader, vaultSecretName);
				}

			}

			storeMessageinStream(streamMessage, DATA_STREAM_OCID, streamKey);

		} catch (BmcException e) {
			LOGGER.severe(e.getLocalizedMessage());
			httpGatewayContext.setStatusCode(e.getStatusCode());
			return e.getLocalizedMessage();

		} catch (JsonProcessingException jsonex) {
			LOGGER.severe(jsonex.getLocalizedMessage());
			httpGatewayContext.setStatusCode(500);
			return "Error occured in processing the payload ";
		}

		return "success";

	}

	/**
	 * @param vaultSecretName
	 * @return boolean
	 * 
	 *         This method is used to get check if secretname is present already in
	 *         vault
	 */
	private boolean checkSecretInVault(String vaultSecretName) {

		VaultsClient vaultClient = new VaultsClient(provider);

		ListSecretsRequest listSecretsRequest = ListSecretsRequest.builder().name(vaultSecretName).vaultId(VAULT_OCID)
				.compartmentId(VAULT_COMPARTMENT_OCID).build();

		ListSecretsResponse listSecretsResponse = vaultClient.listSecrets(listSecretsRequest);
		List<SecretSummary> items = listSecretsResponse.getItems();
		vaultClient.close();
		return items.isEmpty();

	}

	/**
	 * @param authorizationHeader
	 * @param vaultSecretName
	 * 
	 *                            This method is to store the auth token in a vault.
	 *                            It generates a secret with content as auth token
	 *                            and name as the vaultSecretName. The secret is
	 *                            stored in the vault in the compartment specified
	 *                            in application configuration variables. The secret
	 *                            encryption key used is also specified in the
	 *                            application configuration variable.
	 */

	private void createSecretInVault(String authorizationHeader, String vaultSecretName) {
		VaultsClient vaultClient = new VaultsClient(provider);
//Create a new secret with content as the authorization header value and name as vaultSecretName
		Base64SecretContentDetails base64SecretContentDetails = Base64SecretContentDetails.builder()
				.content(authorizationHeader).name(vaultSecretName).stage(SecretContentDetails.Stage.Current).build();

		// The secret is created in the compartment and vault specified in application
		// configuration variable
		// The secret uses the key mentioned in the application configuration variable
		CreateSecretDetails createSecretDetails = CreateSecretDetails.builder().compartmentId(VAULT_COMPARTMENT_OCID)
				.secretName(vaultSecretName).keyId(VAULT_KEY_OCID).vaultId(VAULT_OCID)
				.secretContent(base64SecretContentDetails).build();
		CreateSecretRequest createSecretRequest = CreateSecretRequest.builder().createSecretDetails(createSecretDetails)
				.build();
		vaultClient.createSecret(createSecretRequest);
		vaultClient.close();

	}

	/**
	 * @param streamOCID
	 * @return Stream This method obtains the Stream object from the stream OCID.
	 */
	private Stream getStream(String streamOCID) {
		StreamAdminClient streamAdminClient = StreamAdminClient.builder().build(provider);
		GetStreamResponse getResponse = streamAdminClient
				.getStream(GetStreamRequest.builder().streamId(streamOCID).build());
		return getResponse.getStream();
	}

	/**
	 * @param message
	 * @param streamOCID
	 * @param streamKey
	 * 
	 * 
	 *                   This method stores the message in the Stream
	 */
	private void storeMessageinStream(String message, String streamOCID, String streamKey) {

		StreamClient streamClient = StreamClient.builder().stream(getStream(streamOCID)).build(provider);

		PutMessagesDetails messagesDetails = PutMessagesDetails.builder()
				.messages(Arrays.asList(
						PutMessagesDetailsEntry.builder().key(streamKey.getBytes()).value(message.getBytes()).build()))
				.build();
		PutMessagesRequest putRequest = PutMessagesRequest.builder().streamId(streamOCID)
				.putMessagesDetails(messagesDetails).build();

		PutMessagesResponse putResponse = streamClient.putMessages(putRequest);
		for (PutMessagesResultEntry entry : putResponse.getPutMessagesResult().getEntries()) {
			if (entry.getError() != null) {

				LOGGER.severe("Put message error " + entry.getErrorMessage());
			} else {

				LOGGER.info("Message pushed to offset " + entry.getOffset() + " in partition " + entry.getPartition());
			}
		}

	}
}

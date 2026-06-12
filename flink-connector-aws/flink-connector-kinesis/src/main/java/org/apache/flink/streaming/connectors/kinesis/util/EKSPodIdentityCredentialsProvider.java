/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.util;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;

/**
 * AWS credentials provider that supports EKS Pod Identity.
 *
 * <p>EKS Pod Identity injects AWS_CONTAINER_CREDENTIALS_FULL_URI=http://169.254.170.23/v1/credentials
 * into pods. The default AWS SDK v1 EC2ContainerCredentialsProviderWrapper rejects this URI
 * because it is not a loopback address and not HTTPS. This provider calls the endpoint directly
 * using java.net.HttpURLConnection, bypassing that validation.
 */
public class EKSPodIdentityCredentialsProvider implements AWSCredentialsProvider {

    public static final EKSPodIdentityCredentialsProvider INSTANCE = new EKSPodIdentityCredentialsProvider();

    private static final Logger LOG = LoggerFactory.getLogger(EKSPodIdentityCredentialsProvider.class);

    private static final String POD_IDENTITY_URI_ENV = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
    private static final String POD_IDENTITY_TOKEN_ENV = "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE";
    private static final String POD_IDENTITY_HOST = "169.254.170.23";
    // Refresh credentials 60 seconds before expiration to avoid using expired credentials
    // at the moment of an API call. EKS Pod Identity credentials are temporary STS credentials
    // with an expiration (typically a few hours). Without proactive refresh, long-running Flink
    // jobs would eventually hit ExpiredTokenException. AWS SDK's own providers use the same pattern.
    private static final int REFRESH_BUFFER_SECONDS = 60;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private volatile BasicSessionCredentials credentials;
    private volatile Instant expiration;

    /**
     * Returns true if EKS Pod Identity environment is detected.
     *
     * <p>Checks two conditions:
     * 1. Env var {@code AWS_CONTAINER_CREDENTIALS_FULL_URI} is present.
     * 2. Its value contains {@code 169.254.170.23} (the EKS Pod Identity link-local IP).
     *
     * <p>EKS Pod Identity automatically injects this env var into pods. Non-EKS environments
     * (local dev, Rancher, IRSA) will not have it, so this check reliably detects whether the
     * pod is running under Pod Identity without affecting other credential flows.
     */
    public static boolean isAvailable() {
        String uri = System.getenv(POD_IDENTITY_URI_ENV);
        return uri != null && uri.contains(POD_IDENTITY_HOST);
    }

    @Override
    public AWSCredentials getCredentials() {
        if (needsRefresh()) {
            refresh();
        }
        return credentials;
    }

    @Override
    public void refresh() {
        String uri = System.getenv(POD_IDENTITY_URI_ENV);
        if (uri == null) {
            throw new RuntimeException(POD_IDENTITY_URI_ENV + " environment variable not set");
        }
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(uri).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(2000);

            // Pod Identity requires the authorization token from a file
            String tokenFile = System.getenv(POD_IDENTITY_TOKEN_ENV);
            if (tokenFile != null) {
                String token = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(tokenFile))).trim();
                conn.setRequestProperty("Authorization", token);
            }

            int status = conn.getResponseCode();
            if (status != 200) {
                throw new RuntimeException("Pod Identity endpoint returned HTTP " + status);
            }

            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
            }

            JsonNode node = MAPPER.readTree(sb.toString());
            String accessKeyId = node.get("AccessKeyId").asText();
            String secretAccessKey = node.get("SecretAccessKey").asText();
            String sessionToken = node.get("Token").asText();
            String expirationStr = node.path("Expiration").asText(null);

            credentials = new BasicSessionCredentials(accessKeyId, secretAccessKey, sessionToken);
            expiration = expirationStr != null ? Instant.parse(expirationStr) : Instant.now().plusSeconds(3600);

            LOG.debug("Successfully refreshed EKS Pod Identity credentials, expiration: {}", expiration);

        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve EKS Pod Identity credentials from " + uri, e);
        }
    }

    // Lazy refresh: checked on every getCredentials() call (which the AWS SDK invokes before
    // each API request). No background thread needed — this is sufficient because the SDK
    // calls getCredentials() frequently enough to catch the refresh window.
    private boolean needsRefresh() {
        return credentials == null
                || expiration == null
                || Instant.now().isAfter(expiration.minusSeconds(REFRESH_BUFFER_SECONDS));
    }
}

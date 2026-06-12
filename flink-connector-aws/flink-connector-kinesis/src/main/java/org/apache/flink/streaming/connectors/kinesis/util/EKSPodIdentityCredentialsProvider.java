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
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;

/**
 * AWS credentials provider that supports EKS Pod Identity by delegating to the AWS SDK v2 {@link
 * ContainerCredentialsProvider}, which natively handles EKS Pod Identity including credential
 * refresh, thread safety, retry, and timeout. The v2 credentials are then adapted to the v1 {@link
 * AWSCredentialsProvider} interface required by flink-connector-kinesis.
 *
 * <p>Background: EKS Pod Identity injects credentials via {@code
 * http://169.254.170.23/v1/credentials}. The shaded AWS SDK v1 {@code
 * EC2ContainerCredentialsProviderWrapper} rejects this URI because it is non-loopback and
 * non-HTTPS. SDK v2's {@link ContainerCredentialsProvider} does not have this restriction.
 */
public class EKSPodIdentityCredentialsProvider implements AWSCredentialsProvider {

    public static final EKSPodIdentityCredentialsProvider INSTANCE =
            new EKSPodIdentityCredentialsProvider();

    private static final String POD_IDENTITY_URI_ENV = "AWS_CONTAINER_CREDENTIALS_FULL_URI";

    // Fixed link-local IP of the EKS Pod Identity agent, defined by AWS.
    // ECS also sets AWS_CONTAINER_CREDENTIALS_FULL_URI but uses 169.254.170.2,
    // so this IP distinguishes EKS Pod Identity from all other credential environments.
    private static final String POD_IDENTITY_HOST = "169.254.170.23";

    private final ContainerCredentialsProvider v2Provider;

    private EKSPodIdentityCredentialsProvider() {
        this.v2Provider = ContainerCredentialsProvider.builder().build();
    }

    /**
     * Returns true if EKS Pod Identity environment is detected.
     *
     * <p>Checks two conditions: 1. Env var {@code AWS_CONTAINER_CREDENTIALS_FULL_URI} is present.
     * 2. Its value contains {@code 169.254.170.23} (the fixed link-local IP of the EKS Pod Identity
     * agent).
     *
     * <p>Both conditions must be true. Note that ECS also sets {@code
     * AWS_CONTAINER_CREDENTIALS_FULL_URI} but points to a different IP ({@code 169.254.170.2}), so
     * this check reliably detects EKS Pod Identity without affecting other credential flows (local
     * dev, Rancher, IRSA, ECS).
     */
    public static boolean isAvailable() {
        String uri = System.getenv(POD_IDENTITY_URI_ENV);
        return uri != null && uri.contains(POD_IDENTITY_HOST);
    }

    @Override
    public AWSCredentials getCredentials() {
        // Delegate to SDK v2 ContainerCredentialsProvider which handles credential refresh,
        // thread safety, retry, and timeout out of the box.
        AwsSessionCredentials creds = (AwsSessionCredentials) v2Provider.resolveCredentials();
        return new BasicSessionCredentials(
                creds.accessKeyId(), creds.secretAccessKey(), creds.sessionToken());
    }

    @Override
    public void refresh() {
        // Intentionally empty — refresh is handled automatically by the SDK v2
        // ContainerCredentialsProvider via its internal CachedSupplier.
    }
}

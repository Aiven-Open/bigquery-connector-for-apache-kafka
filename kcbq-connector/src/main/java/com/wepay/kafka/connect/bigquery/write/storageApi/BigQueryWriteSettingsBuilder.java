package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class BigQueryWriteSettingsBuilder extends GcpClientBuilder<BigQueryWriteSettings> {

    private static final Logger logger = LoggerFactory.getLogger(BigQueryWriteSettingsBuilder.class);

    @Override
    protected BigQueryWriteSettings doBuild(String project, GoogleCredentials credentials, HeaderProvider userAgent) {
        BigQueryWriteSettings.Builder builder = BigQueryWriteSettings.newBuilder()
                .setQuotaProjectId(project)
                .setHeaderProvider(userAgent);


        if (credentials != null) {
            builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        } else {
            logger.warn("Attempting to access GCS without authentication");
        }
        try {
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

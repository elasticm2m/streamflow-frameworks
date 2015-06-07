package com.elasticm2m.frameworks.aws.credentials;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.elasticm2m.frameworks.common.base.ElasticBaseResource;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.lang.StringUtils;

public class AWSCredentialsResource extends ElasticBaseResource {
    
    private String accessKey;
    
    private String secretKey;
    
    @Inject(optional=true)
    public void setAccessKey(@Named("aws-access-key") String accessKey) {
        this.accessKey = accessKey;
    }
    
    @Inject(optional=true)
    public void setSecretKey(@Named("aws-secret-key") String secretKey) {
        this.secretKey = secretKey;
    }

    @Override
    public void configure() {
        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
            logger.info("Using custom AWS credentials provider");
            
            bind(AWSCredentialsProvider.class).toInstance(new AWSCredentialsProvider() {
                @Override
                public AWSCredentials getCredentials() {
                    return new BasicAWSCredentials(accessKey, secretKey);
                }

                @Override
                public void refresh() {
                    // NOOP
                }
            });
        } else {
            logger.info("Using default AWS credentials provider");
            
            bind(AWSCredentialsProvider.class).to(DefaultAWSCredentialsProviderChain.class);
        }
    }
}

package com.elasticm2m.frameworks.twitter;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichSpout;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.lang.StringUtils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStreamingSpout extends ElasticBaseRichSpout {

    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;
    private String proxyHost;
    private int proxyPort;

    private final LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(100000);
    private TwitterStream twitterStream;

    @Inject
    public void setConsumerKey(@Named("oauth-consumer-key") String consumerKey) {
        this.consumerKey = consumerKey;
    }

    @Inject
    public void setConsumerSecret(@Named("oauth-consumer-secret") String consumerSecret) {
        this.consumerSecret = consumerSecret;
    }

    @Inject
    public void setAccessToken(@Named("oauth-access-token") String accessToken) {
        this.accessToken = accessToken;
    }

    @Inject
    public void setAccessTokenSecret(@Named("oauth-access-token-secret") String accessTokenSecret) {
        this.accessTokenSecret = accessTokenSecret;
    }

    @Inject(optional = true)
    public void setProxyHost(@Named("http.proxy.host") String proxyHost) {
        this.proxyHost = proxyHost;
    }

    @Inject(optional = true)
    public void setProxyPort(@Named("http.proxy.port") int proxyPort) {
        this.proxyPort = proxyPort;
    }

    @Override
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        logger.info("Twitter Sampler Started: Consumer Key = " + consumerKey
                + ", Consumer Secret = " + consumerSecret + ", Access Token = " + accessToken
                + ", Access Token Secret = " + accessTokenSecret);

        if (StringUtils.isNotBlank(consumerKey) && StringUtils.isNotBlank(consumerSecret) &&
                StringUtils.isNotBlank(accessToken) && StringUtils.isNotBlank(accessTokenSecret)) {
            // Build the twitter config to authenticate the requests
            ConfigurationBuilder twitterConfig = new ConfigurationBuilder()
                    .setOAuthConsumerKey(consumerKey)
                    .setOAuthConsumerSecret(consumerSecret)
                    .setOAuthAccessToken(accessToken)
                    .setOAuthAccessTokenSecret(accessTokenSecret)
                    .setJSONStoreEnabled(true)
                    .setIncludeEntitiesEnabled(true)
                    .setIncludeEntitiesEnabled(true);

            // Add the proxy settings to the Twitter config if they were specified
            if (StringUtils.isNotBlank(proxyHost) && proxyPort > 0) {
                try {
                    twitterConfig.setHttpProxyPort(proxyPort).setHttpProxyHost(proxyHost);
                } catch (Exception ex) {
                }
            }

            // Status listener which handle the status events and add them to the queue
            StatusListener listener = new StatusListener() {
                @Override
                public void onStatus(Status status) {
                    String json = TwitterObjectFactory.getRawJSON(status);
                    queue.offer(json);
                }

                @Override
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                    logger.debug("Twitter Deletion Notice: " + statusDeletionNotice.getUserId());
                }

                @Override
                public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                    logger.debug("Twitter On Track Limitation Notice: Number Of Limited Statuses"
                            + numberOfLimitedStatuses);
                }

                @Override
                public void onScrubGeo(long userId, long upToStatusId) {
                    logger.debug("Twitter Scrub Geo: UserID = " + userId
                            + ", UpToStatusId = " + upToStatusId);
                }

                @Override
                public void onException(Exception exception) {
                    logger.debug("Twitter Exception: " + exception.getMessage());
                }

                @Override
                public void onStallWarning(StallWarning stallWarning) {
                    logger.debug("Twitter Stall Warning: " + stallWarning.toString());
                }
            };

            TwitterStreamFactory twitterFactory = new TwitterStreamFactory(twitterConfig.build());
            twitterStream = twitterFactory.getInstance();
            twitterStream.addListener(listener);
            twitterStream.sample();

            logger.info("Twitter Sample Stream Initialized");

        } else {
            logger.info("Twitter Sampler missing required OAuth properties. "
                    + "Pleast check your settings and try again.");
        }
    }

    @Override
    public void nextTuple() {
        String status = queue.poll();

        if (status == null) {
            Utils.sleep(50);
        } else {
            // Emit the twitter status as a JSON String
            try {
                collector.emit(statusToTuple(status));
            } catch (JsonProcessingException e) {
                logger.error("Error emitting status", e);
            }
        }
    }

    @Override
    public void close() {
        if (twitterStream != null) {
            twitterStream.shutdown();
        }

        logger.info("Twitter Stream Stopped");
    }

    public List<Object> statusToTuple(String status) throws JsonProcessingException {
        HashMap<String, String> properties = new HashMap<>();
        List<Object> tuple = new ArrayList<>();
        tuple.add(null);
        tuple.add(status);
        tuple.add(properties);
        return tuple;
    }

}

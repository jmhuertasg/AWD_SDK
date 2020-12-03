package com.ftm.bigdata;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;

public class KinesisFirehoseClient {

    public static final String AWS_ACCESS_KEY_ID = "aws.accessKeyId";
    public static final String AWS_SECRET_KEY = "aws.secretKey";

    static {
        System.setProperty(AWS_ACCESS_KEY_ID, "&&&&");
        System.setProperty(AWS_SECRET_KEY, "&&&&");
    }

    public static AmazonKinesisFirehose getFirehoseClient(){
        return AmazonKinesisFirehoseClientBuilder.standard().withRegion(Regions.EU_WEST_1   )
                .build();
    }
}

package upf.edu.uploader;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;


import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.logging.*;
import java.lang.Exception;
import org.apache.logging.log4j.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.tools.Generate;

public class S3Uploader implements Uploader{
    private String bucketName;
    private String prefix;
    private String domain;

    public S3Uploader(String bucket, String prefix, String domain){
        this.bucketName = bucket;
        this.prefix = prefix;
        this.domain = domain;
    }

    @Override
    public void upload(List<String> files) {

        //Benchmark Uploader
        long startTimeUploader = System.nanoTime();
        //
        Logger logger = LogManager.getLogger(S3Uploader.class.getName());

        //Initialize client with the specified credentials
        AmazonS3 s3client = AmazonS3ClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .build();
        System.out.println("logged correctly");
        //Case for when the bucket does not exist
        if(!s3client.doesBucketExistV2(this.bucketName)) {
            try{
                s3client.createBucket(this.bucketName);
            }
            //If the bucket could not be created successfully throw an exception
            catch (Exception e){
                logger.error(e.getMessage());
            }
        }
        //For every given file, push it to the bucket
        for (String file: files) {
            s3client.putObject(
                    this.bucketName,
                    this.prefix,
                    new File(file));
        }
        //Print time for upload
        System.out.println("Uploader time: " + (float)(System.nanoTime() - startTimeUploader) / 1000000000 + " s");
    }
}

package org.jandy.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.util.IOUtils;
import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.rapidoid.lambda.ThreeParamLambda;
import org.rapidoid.log.Log;
import org.rapidoid.setup.On;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static org.apache.commons.io.FileUtils.ONE_MB;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;

public class Main {

    private static final String UPLOAD_FILE_NAME = "storage-performance-test-file";

    public static void main(String[] args) {
        S3Uploader uploader = new S3Uploader();
        On.get("/upload/{type}").json((ThreeParamLambda<Object, String, String, Integer>) uploader::upload);
    }

    static class S3Uploader {

        private AmazonS3 s3Client;
        private String bucketName;
        private String keyName;
        private AtomicInteger counter = new AtomicInteger();

        S3Uploader() {
            AWSCredentials credentials = new BasicAWSCredentials("accessKey", "secretKey");
            s3Client = AmazonS3ClientBuilder.standard()
                                    .withRegion(Regions.EU_WEST_1)
                                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                                    .build();
            bucketName = "bucket-name";
        }

        String upload(String type, String fileName, Integer fileSizeMb) throws Exception {
            InputStream is;
            if (fileSizeMb != null) {
                byte[] input = new byte[(int) (fileSizeMb * FileUtils.ONE_MB)];
                Arrays.fill(input, (byte) 1);
                is = new ByteArrayInputStream(input);
            } else {
                is = new FileInputStream(fileName);
            }

            String response = null;
            if (type.equals("s3")) {
                response = upload(is);
            }
            return response;
        }

        String upload(InputStream is) throws Exception {
            keyName = UPLOAD_FILE_NAME + counter.incrementAndGet();

            List<PartETag> partETags = new ArrayList<>();
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, keyName);
            InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

            int partSize = (int) (5 * ONE_MB); // Min part size is 5 MB
            byte[] buffer = new byte[partSize];
            int bytesRead;
            int partNum = 1;

            Log.info("Starting upload", "bucket", bucketName, "key", keyName);
            Stopwatch timer = Stopwatch.createStarted();
            for (int i = 1; (bytesRead = is.read(buffer)) != -1; i++) {
                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(keyName)
                        .withUploadId(initResponse.getUploadId())
                        .withPartNumber(partNum)
                        .withInputStream(new ByteArrayInputStream(buffer))
                        .withPartSize(partSize);

                Log.info("Uploading part", "partSize", byteCountToDisplaySize(bytesRead), "partNum", partNum);
                if (bytesRead == partSize) {
                    partNum++;
                } else { // len < partSize so we are done
                    uploadRequest.withLastPart(true);
                }

                partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());
            }
            is.close();

            CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
                    bucketName,
                    keyName,
                    initResponse.getUploadId(),
                    partETags);
            CompleteMultipartUploadResult uploadResult = s3Client.completeMultipartUpload(compRequest);
            String location = uploadResult.getLocation();
            Log.info("Uploaded", "location", location, "timer", timer);

            timer.stop();
            return format("Sent %s parts in %s", partNum, timer);
        }

        private void deleteUpload() {
            s3Client.deleteObject(bucketName, keyName);
        }

        private void validate(String inputString) throws IOException {
            S3ObjectInputStream objectContent = s3Client.getObject(bucketName, keyName).getObjectContent();
            String theString = IOUtils.toString(objectContent);
            objectContent.close();
            if (!Objects.equals(inputString, theString)) {
                throw new RuntimeException("uploaded content corrupted");
            }
        }
    }
}

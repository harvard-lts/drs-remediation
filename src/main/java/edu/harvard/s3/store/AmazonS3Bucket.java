/**
 * Copyright (c) 2021 President and Fellows of Harvard College
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.harvard.s3.store;

import static edu.harvard.s3.utility.RuntimeUtils.totalMemory;
import static edu.harvard.s3.utility.TimeUtils.ellapsed;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.removeEnd;
import static org.apache.commons.lang3.StringUtils.removeStart;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectResult;
import software.amazon.awssdk.services.s3.model.CopyPartResult;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

/**
 * Amazon S3 object store implementation scoped to a specific bucket.
 */
@Slf4j
public class AmazonS3Bucket implements ObjectStore {

    private static final long MAX_PART_SIZE = 5 * 1024 * 1024;

    private final S3Client s3;

    private final String bucketName;

    private final int maxKeys;

    /**
     * Amazon S3 bucket object store constructor.
     *
     * @param bucketName       AWS bucket name
     * @param maxKeys          max keys for listing objects
     * @param endpointOverride AWS endpoint override
     */
    public AmazonS3Bucket(String bucketName, int maxKeys, String endpointOverride) {
        S3ClientBuilder builder = S3Client.builder();
        if (Objects.nonNull(endpointOverride)) {
            try {
                builder = builder.endpointOverride(new URI(endpointOverride));
            } catch (URISyntaxException e) {
                throw new RuntimeException("Unable to build endpoint override URI", e);
            }
        }
        this.s3 = builder.build();
        this.bucketName = bucketName;
        this.maxKeys = maxKeys;
    }

    @Override
    public int count() {
        return list().stream()
            .map(r -> r.contents())
            .map(p -> p.size())
            .mapToInt(Integer::valueOf)
            .sum();
    }

    @Override
    public List<List<S3Object>> partition() {
        log.info("paritioning objects in bucket {}", bucketName);
        long startTime = System.nanoTime();

        ListObjectsV2Iterable iterable = list();

        List<List<S3Object>> partitions = iterable.stream()
            .map(r -> r.contents())
            .collect(Collectors.toList());

        log.info("{} partitions for bucket {}", partitions.size(), bucketName);

        int count = partitions.stream()
            .map(p -> p.size())
            .mapToInt(Integer::valueOf)
            .sum();

        log.info("{} objects in bucket {}", count, bucketName);
        log.debug("{} milliseconds to partition objects", ellapsed(startTime));
        log.debug("{} GiB total memory used after partitions", totalMemory());

        return partitions;
    }

    @Override
    public int rename(S3Object source, String destinationKey) {
        if (source.size() >= 5368709120L) {
            log.warn("skipping copy: source object {} is over 5 GiB copy limit at {} bytes",
                source.key(), source.size());
            return 1;
        }

        String sourceEtag = normalizeEtag(source.eTag());

        // String destiationEtag = source.size() < 5368709120L
        //     ? copy(source, destinationKey)
        //     : multiPartCopy(source, destinationKey);

        try {
            String destiationEtag = copy(source, destinationKey);

            if (!destiationEtag.equals(sourceEtag)) {
                log.warn("copy failure: source etag {} does not match destination etag {}",
                    sourceEtag, destiationEtag);
                return -1;
            }

            log.debug("copy success: source object {} to destination object {}",
                source.key(), destinationKey);
        } catch (SdkClientException | S3Exception e) {
            log.error("Error while attempting to copy object", e);
            return -1;
        }

        try {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(this.bucketName)
                .key(source.key())
                .build();

            s3.deleteObject(deleteObjectRequest);
        } catch (SdkClientException | S3Exception e) {
            log.error("Error while attempting to delete object", e);
            return -1;
        }

        return 0;
    }

    @Override
    public void close() {
        this.s3.close();
    }

    private ListObjectsV2Iterable list() {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
            .bucket(this.bucketName)
            .maxKeys(this.maxKeys)
            .build();

        return this.s3.listObjectsV2Paginator(request);
    }

    private String copy(S3Object source, String destinationKey) {
        CopyObjectRequest copyObjectRequest = CopyObjectRequest.builder()
            .sourceBucket(this.bucketName)
            .sourceKey(source.key())
            .destinationBucket(this.bucketName)
            .destinationKey(destinationKey)
            .build();

        CopyObjectResponse response = s3.copyObject(copyObjectRequest);

        CopyObjectResult result = response.copyObjectResult();

        return normalizeEtag(result.eTag());
    }

    private String multiPartCopy(S3Object source, String destinationKey) {
        CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
            .bucket(this.bucketName)
            .key(destinationKey)
            .build();

        CreateMultipartUploadResponse createResponse = s3.createMultipartUpload(createRequest);

        List<CompletedPart> completedParts = new ArrayList<>();

        String uploadId = createResponse.uploadId();

        int partNumber = 1;

        for (long pos = 0; pos < source.size(); pos += MAX_PART_SIZE) {
            String copySourceRange = copySourceRange(pos, source.size());

            UploadPartCopyRequest partRequest = UploadPartCopyRequest.builder()
                .sourceBucket(this.bucketName)
                .sourceKey(source.key())
                .destinationBucket(this.bucketName)
                .destinationKey(destinationKey)
                .copySourceRange(copySourceRange)
                .partNumber(partNumber)
                .uploadId(uploadId)
                .build();

            UploadPartCopyResponse partResponse = s3.uploadPartCopy(partRequest);

            CopyPartResult copyPartResult = partResponse.copyPartResult();

            CompletedPart completedPart = CompletedPart.builder()
                .eTag(copyPartResult.eTag())
                .partNumber(partNumber)
                .build();

            completedParts.add(completedPart);

            partNumber++;
        }

        CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
            .parts(completedParts)
            .build();

        CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
            .bucket(this.bucketName)
            .key(destinationKey)
            .uploadId(uploadId)
            .multipartUpload(completedMultipartUpload)
            .build();

        CompleteMultipartUploadResponse completeResponse = s3.completeMultipartUpload(completeRequest);

        return normalizeEtag(completeResponse.eTag());
    }

    private String copySourceRange(long start, long size) {
        long end = start + MAX_PART_SIZE - 1;
        if (end > size) {
            end = size - 1;
        }
        return format("bytes=%s-%s", start, end);
    }

    private String normalizeEtag(String etag) {
        return removeEnd(removeStart(etag, "\""), "\"");
    }

}

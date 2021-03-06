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

package edu.harvard.drs.remediation.store;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.removeEnd;
import static org.apache.commons.lang3.StringUtils.removeStart;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
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

    private final S3Client s3;

    private final String bucketName;

    private final int maxKeys;

    private final long maxPartSize;

    private final long multipartThreshold;

    private final boolean skipMultipart;

    /**
     * Amazon S3 bucket object store constructor.
     *
     * @param bucketName         AWS bucket name
     * @param maxKeys            max keys for listing objects
     * @param maxPartSize        max part size for multipart upload
     * @param multipartThreshold multipart threshold
     * @param skipMultipart      whether to skip multipart
     * @param endpointOverride   AWS endpoint override
     */
    public AmazonS3Bucket(
        String bucketName,
        int maxKeys,
        long maxPartSize,
        long multipartThreshold,
        boolean skipMultipart,
        String endpointOverride
    ) {
        S3ClientBuilder builder = S3Client.builder();
        if (Objects.nonNull(endpointOverride)) {
            builder = builder.endpointOverride(URI.create(endpointOverride));
        }
        this.s3 = builder.build();
        this.bucketName = bucketName;
        this.maxKeys = maxKeys;
        this.maxPartSize = maxPartSize;
        this.multipartThreshold = multipartThreshold;
        this.skipMultipart = skipMultipart;
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
    public Iterator<List<S3Object>> iterator() {
        log.info("iterator of objects in bucket {}", bucketName);

        Iterator<ListObjectsV2Response> iterator = list().iterator();

        return new Iterator<List<S3Object>>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public List<S3Object> next() {
                return iterator.next()
                    .contents();
            }

        };
    }

    @Override
    public int rename(S3Object source, String destinationKey) {
        try {
            int copyResult;
            if (source.size() < multipartThreshold && !source.eTag().contains("-")) {
                copyResult = copy(source, destinationKey);
            } else {
                copyResult = skipMultipart ? 1 : multiPartCopy(source, destinationKey);
            }

            if (copyResult == 0) {
                log.debug("copy success: source object {} to destination object {}",
                    source.key(), destinationKey);
            } else {
                return copyResult;
            }
        } catch (SdkClientException | S3Exception e) {
            log.error("Error while attempting to copy object", e);
            return -1;
        }

        try {
            delete(source);
        } catch (SdkClientException | S3Exception e) {
            log.error("Error while attempting to delete object", e);
            return -1;
        }

        return 0;
    }

    @Override
    public void close() {
        try {
            this.s3.close();
        } catch (Exception e) {
            log.error("Error while attempting to close s3 client", e);
        }
    }

    private ListObjectsV2Iterable list() {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
            .bucket(this.bucketName)
            .maxKeys(this.maxKeys)
            .build();

        return this.s3.listObjectsV2Paginator(request);
    }

    private int copy(S3Object source, String destinationKey) {
        CopyObjectRequest copyObjectRequest = CopyObjectRequest.builder()
            .sourceBucket(this.bucketName)
            .sourceKey(source.key())
            .destinationBucket(this.bucketName)
            .destinationKey(destinationKey)
            .build();

        CopyObjectResponse response = s3.copyObject(copyObjectRequest);

        CopyObjectResult result = response.copyObjectResult();

        String sourceEtag = normalizeEtag(source.eTag());
        String destinationEtag = normalizeEtag(result.eTag());

        if (sourceEtag.equals(destinationEtag)) {
            return 0;
        } else {
            log.error("copy failure: source etag {} does not match destination etag {}",
                sourceEtag, destinationEtag);
            return -2;
        }
    }

    private int multiPartCopy(S3Object source, String destinationKey) {
        CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
            .bucket(this.bucketName)
            .key(destinationKey)
            .build();

        CreateMultipartUploadResponse createResponse = s3.createMultipartUpload(createRequest);

        String uploadId = createResponse.uploadId();

        List<ObjectPart> parts = new ArrayList<>();

        int partNumber = 0;

        for (long position = 0; position < source.size(); position += maxPartSize) {
            parts.add(new ObjectPart(++partNumber, position));
        }

        List<CompletedPart> completedParts = parts.parallelStream()
            .map(part -> {
                String copySourceRange = copySourceRange(part.getPosition(), source.size());

                UploadPartCopyRequest partRequest = UploadPartCopyRequest.builder()
                    .sourceBucket(this.bucketName)
                    .sourceKey(source.key())
                    .destinationBucket(this.bucketName)
                    .destinationKey(destinationKey)
                    .copySourceRange(copySourceRange)
                    .partNumber(part.getNumber())
                    .uploadId(uploadId)
                    .build();

                UploadPartCopyResponse partResponse = s3.uploadPartCopy(partRequest);

                CopyPartResult copyPartResult = partResponse.copyPartResult();

                return CompletedPart.builder()
                    .eTag(normalizeEtag(copyPartResult.eTag()))
                    .partNumber(part.getNumber())
                    .build();
            }).collect(Collectors.toList());

        Collections.sort(completedParts, new CompletedPartComparator());

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

        String destinationEtag = normalizeEtag(completeResponse.eTag());

        int etagPartCount = parseInt(destinationEtag.split("-")[1], 10);

        if (etagPartCount == partNumber) {
            return 0;
        } else {
            log.error("copy failure: destination etag {} did not match expected number of parts {}",
                destinationEtag, partNumber);
            return -2;
        }
    }

    private void delete(S3Object object) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
            .bucket(this.bucketName)
            .key(object.key())
            .build();

        s3.deleteObject(deleteObjectRequest);
    }

    private String copySourceRange(long start, long size) {
        long end = Math.min(start + maxPartSize - 1, size - 1);

        return format("bytes=%d-%d", start, end);
    }

    private String normalizeEtag(String etag) {
        return removeEnd(removeStart(etag, "\""), "\"");
    }

}

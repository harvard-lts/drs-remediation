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

import static edu.harvard.s3.utility.EnvUtils.getAwsBucketName;
import static edu.harvard.s3.utility.EnvUtils.getAwsMaxPartSize;
import static edu.harvard.s3.utility.EnvUtils.getInputPattern;
import static edu.harvard.s3.utility.EnvUtils.getInputSkip;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.removeEnd;
import static org.apache.commons.lang3.StringUtils.removeStart;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import edu.harvard.s3.loader.FileLoader;
import io.netty.util.internal.ThreadLocalRandom;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

/**
 * Abstract store test for integration S3 testing.
 */
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(S3MockExtension.class)
public abstract class AbstractStoreTest {

    protected final String inputPath = "src/test/resources/dump.txt";

    protected final String endpointOverride = "http://localhost:9090";

    @BeforeAll
    void setup(final S3Client s3) {
        CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
            .bucket(getAwsBucketName())
            .build();

        s3.createBucket(createBucketRequest);

        final String path = "src/test/resources/dump.txt";
        final String pattern = getInputPattern();
        final int skip = getInputSkip();

        FileLoader loader = new FileLoader(path, pattern, skip);

        List<SimpleEntry<String, String>> entries = loader.load()
            .collect(Collectors.toList());

        File mets = new File("src/test/resources/objects/0492131461/v1/content/descriptor/492131461_mets.xml");
        File mods = new File("src/test/resources/objects/0492131461/v1/content/metadata/492131461_mods.xml");
        File png = new File("src/test/resources/objects/0492131461/v1/content/data/492131461.png");

        List<String> ids = entries.stream()
            .map(e -> e.getKey())
            .collect(Collectors.toList());

        for (String id : ids) {
            putObject(s3, mets, format("0%1$s/v1/content/descriptor/%1$s_mets.xml", id));
            putObject(s3, mods, format("0%1$s/v1/content/metadata/%1$s_mods.xml", id));
            putObject(s3, png, format("0%1$s/v1/content/data/%1$s.png", id));

            String filename = format("target/%1$s.lfs", id);
            long sizeInBytes = random(104857600L, 262144000L);

            File lfs = createLargeFile(filename, sizeInBytes);
            lfs.deleteOnExit();

            multipartUpload(s3, lfs, format("0%1$s/v1/content/data/%1$s.lfs", id));
        }

        s3.close();
    }

    @AfterAll
    void cleanup(final S3Client s3) {
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
            .bucket(getAwsBucketName())
            .build();

        ListObjectsV2Iterable iterable = s3.listObjectsV2Paginator(listObjectsV2Request);

        List<ObjectIdentifier> identifiers = iterable.contents().stream()
            .map(o -> ObjectIdentifier.builder().key(o.key()).build())
            .collect(Collectors.toList());

        Delete delete = Delete.builder()
            .objects(identifiers)
            .build();

        DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
            .bucket(getAwsBucketName())
            .delete(delete)
            .build();

        DeleteObjectsResponse response = s3.deleteObjects(deleteObjectsRequest);

        assertTrue(response.hasDeleted());
        assertFalse(response.hasErrors());

        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
            .bucket(getAwsBucketName())
            .build();

        s3.deleteBucket(deleteBucketRequest);

        s3.close();
    }

    private void putObject(final S3Client s3, File file, String key) {
        PutObjectRequest metsObjectRequest = PutObjectRequest.builder()
            .bucket(getAwsBucketName())
            .key(key)
            .build();

        PutObjectResponse metsObjectResponse = s3.putObject(metsObjectRequest, RequestBody.fromFile(file));

        assertEquals(md5Hex(file), normalizeEtag(metsObjectResponse.eTag()));
    }

    private void multipartUpload(final S3Client s3, File file, String key) {
        CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
            .bucket(getAwsBucketName())
            .key(key)
            .build();

        CreateMultipartUploadResponse createResponse = s3.createMultipartUpload(createRequest);

        List<CompletedPart> completedParts = new ArrayList<>();

        String uploadId = createResponse.uploadId();

        List<ObjectPart> parts = new ArrayList<>();

        List<Md5Part> md5Parts = new ArrayList<>();

        int partNumber = 0;

        for (long position = 0; position < file.length(); position += getAwsMaxPartSize()) {
            parts.add(new ObjectPart(++partNumber, position));
        }

        parts.parallelStream()
            .forEach(part -> {
                long length = Math.min(getAwsMaxPartSize(), file.length() - part.getPosition());
                byte[] bytes = readByteRange(file.getAbsolutePath(), part.getPosition(), (int) length);

                md5Parts.add(new Md5Part(part.getNumber(), md5(bytes)));

                UploadPartRequest partRequest = UploadPartRequest.builder()
                    .bucket(getAwsBucketName())
                    .key(key)
                    .partNumber(part.getNumber())
                    .uploadId(uploadId)
                    .build();

                UploadPartResponse partResponse = s3.uploadPart(partRequest, RequestBody.fromBytes(bytes));

                CompletedPart completedPart = CompletedPart.builder()
                    .eTag(normalizeEtag(partResponse.eTag()))
                    .partNumber(part.getNumber())
                    .build();

                completedParts.add(completedPart);
            });

        Collections.sort(completedParts, new Comparator<CompletedPart>() {

            @Override
            public int compare(CompletedPart cp1, CompletedPart cp2) {
                return cp1.partNumber().compareTo(cp2.partNumber());
            }

        });

        Collections.sort(md5Parts, new Comparator<Md5Part>() {

            @Override
            public int compare(Md5Part p1, Md5Part p2) {
                return p1.getNumber().compareTo(p2.getNumber());
            }

        });

        final CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
            .parts(completedParts)
            .build();

        final CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
            .bucket(getAwsBucketName())
            .key(key)
            .uploadId(uploadId)
            .multipartUpload(completedMultipartUpload)
            .build();

        final CompleteMultipartUploadResponse completeResponse = s3.completeMultipartUpload(completeRequest);

        byte[] allMd5s = new byte[0];

        for (Md5Part md5Part : md5Parts) {
            allMd5s = ArrayUtils.addAll(allMd5s, md5Part.md5);
        }

        final String etag = format("%s-%d", DigestUtils.md5Hex(allMd5s), parts.size());

        assertEquals(etag, normalizeEtag(completeResponse.eTag()));
    }

    private File createLargeFile(final String filename, final long sizeInBytes) {
        try {
            File file = new File(filename);
            file.createNewFile();

            try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
                randomAccessFile.setLength(sizeInBytes);
                randomAccessFile.close();

                return file;
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private byte[] readByteRange(String filePath, long start, int length) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "r")) {
            byte[] buffer = new byte[length];
            randomAccessFile.seek(start);
            randomAccessFile.readFully(buffer);

            return buffer;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private String md5Hex(File file) {
        try (InputStream is = new FileInputStream(file)) {
            return DigestUtils.md5Hex(is);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private byte[] md5(byte[] bytes) {
        return DigestUtils.md5(bytes);
    }

    private long random(long low, long high) {
        return ThreadLocalRandom.current().nextLong(low, high);
    }

    private String normalizeEtag(String etag) {
        return removeEnd(removeStart(etag, "\""), "\"");
    }

    @Data
    private class Md5Part {
        private final Integer number;
        private final byte[] md5;
    }

}

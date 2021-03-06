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

import static edu.harvard.drs.remediation.utility.EnvUtils.getAwsBucketName;
import static edu.harvard.drs.remediation.utility.EnvUtils.getAwsMaxPartSize;
import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.commons.lang3.StringUtils.removeEnd;
import static org.apache.commons.lang3.StringUtils.removeStart;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import io.netty.util.internal.ThreadLocalRandom;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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

    protected final String endpointOverride = "http://localhost:9090";

    @BeforeAll
    void setup(final S3Client s3) {
        CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
            .bucket(getAwsBucketName())
            .build();

        s3.createBucket(createBucketRequest);

        File mets = new File("src/test/resources/objects/0492131461/v1/content/descriptor/492131461_mets.xml");
        File mods = new File("src/test/resources/objects/0492131461/v1/content/metadata/492131461_mods.xml");
        File png = new File("src/test/resources/objects/0492131461/v1/content/data/492131461.png");

        List<String[]> ids = Arrays.asList(new String[][] {
            { "12887301", "400171130" },
            { "12887302", "400171132" },
            { "12887305", "400171138" },
            { "12887296", "400171120" },
            { "12887299", "400171126" },
        });

        for (String[] id : ids) {
            putObject(s3, mets, format("%s/v1/content/descriptor/%s_mets.xml", id[0], id[1]));
            putObject(s3, mods, format("%s/v1/content/metadata/%s_mods.xml", id[0], id[1]));
            putObject(s3, png, format("%s/v1/content/data/%s.png", id[0], id[1]));

            String filePath = format("target/%s.lfs", id[0], id[1]);
            long sizeInBytes = random(104857600L, 262144000L);

            File lfs = createLargeFile(filePath, sizeInBytes);

            multipartUpload(s3, lfs, format("%s/v1/content/data/%s.lfs", id[0], id[1]));

            lfs.delete();

            assertFalse(lfs.exists());
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
        PutObjectRequest request = PutObjectRequest.builder()
            .bucket(getAwsBucketName())
            .key(key)
            .build();

        PutObjectResponse response = s3.putObject(request, RequestBody.fromFile(file));

        assertEquals(md5Hex(file), normalizeEtag(response.eTag()));
    }

    private void multipartUpload(final S3Client s3, File file, String key) {
        CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
            .bucket(getAwsBucketName())
            .key(key)
            .build();

        CreateMultipartUploadResponse createResponse = s3.createMultipartUpload(createRequest);

        String uploadId = createResponse.uploadId();

        List<ObjectPart> parts = new ArrayList<>();

        int partNumber = 0;

        for (long position = 0; position < file.length(); position += getAwsMaxPartSize()) {
            parts.add(new ObjectPart(++partNumber, position));
        }

        List<CompletedMd5Part> completedMd5Parts = parts.parallelStream()
            .map(part -> {
                long length = Math.min(getAwsMaxPartSize(), file.length() - part.getPosition());
                byte[] bytes = readByteRange(file.getAbsolutePath(), part.getPosition(), (int) length);

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

                return new CompletedMd5Part(completedPart, part.getNumber(), md5(bytes));
                
            }).collect(Collectors.toList());

        Collections.sort(completedMd5Parts, new CompletedMd5PartComparator());

        List<CompletedPart> completedParts = completedMd5Parts.stream()
            .map(cmp -> cmp.getCompleted())
            .collect(Collectors.toList());

        CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
            .parts(completedParts)
            .build();

        CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
            .bucket(getAwsBucketName())
            .key(key)
            .uploadId(uploadId)
            .multipartUpload(completedMultipartUpload)
            .build();

        CompleteMultipartUploadResponse completeResponse = s3.completeMultipartUpload(completeRequest);

        byte[] allMd5s = new byte[0];

        for (CompletedMd5Part md5Part : completedMd5Parts) {
            allMd5s = ArrayUtils.addAll(allMd5s, md5Part.getMd5());
        }

        String etag = format("%s-%d", DigestUtils.md5Hex(allMd5s), parts.size());

        assertEquals(etag, normalizeEtag(completeResponse.eTag()));
    }

    private File createLargeFile(String filePath, long sizeInBytes) {
        ByteBuffer buf = ByteBuffer.allocate(4)
            .putInt(2);
        buf.rewind();

        OpenOption[] options = { WRITE, CREATE_NEW, SPARSE };
        Path path = Paths.get(filePath);

        try (SeekableByteChannel channel = Files.newByteChannel(path, options)) {
            channel.position(sizeInBytes);
            channel.write(buf);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return path.toFile();
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
    private class CompletedMd5Part {
        private final CompletedPart completed;
        private final Integer number;
        private final byte[] md5;
    }

    private class CompletedMd5PartComparator implements Comparator<CompletedMd5Part> {

        @Override
        public int compare(CompletedMd5Part cmp1, CompletedMd5Part cmp2) {
            return cmp1.getNumber().compareTo(cmp2.getNumber());
        }

    }

}

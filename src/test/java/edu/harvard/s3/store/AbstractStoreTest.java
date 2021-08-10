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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
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
    void setup(final S3Client s3) throws IOException, NoSuchAlgorithmException {
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
            String metskey = format("0%1$s/v1/content/descriptor/%1$s_mets.xml", id);

            PutObjectRequest metsObjectRequest = PutObjectRequest.builder()
                .bucket(getAwsBucketName())
                .key(metskey)
                .build();

            PutObjectResponse metsObjectResponse = s3.putObject(metsObjectRequest, RequestBody.fromFile(mets));

            assertEquals(md5(mets), normalizeEtag(metsObjectResponse.eTag()));

            String modskey = format("0%1$s/v1/content/metadata/%1$s_mods.xml", id);

            PutObjectRequest modsObjectRequest = PutObjectRequest.builder()
                .bucket(getAwsBucketName())
                .key(modskey)
                .build();

            PutObjectResponse modsObjectResponse = s3.putObject(modsObjectRequest, RequestBody.fromFile(mods));

            assertEquals(md5(mods), normalizeEtag(modsObjectResponse.eTag()));

            String pngkey = format("0%1$s/v1/content/data/%1$s.png", id);

            PutObjectRequest pngObjectRequest = PutObjectRequest.builder()
                .bucket(getAwsBucketName())
                .key(pngkey)
                .build();

            PutObjectResponse pngObjectResponse = s3.putObject(pngObjectRequest, RequestBody.fromFile(png));

            assertEquals(md5(png), normalizeEtag(pngObjectResponse.eTag()));
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

    private String md5(File file) throws IOException {
        return DigestUtils.md5Hex(FileUtils.openInputStream(file));
    }

    private String normalizeEtag(String etag) {
        return removeEnd(removeStart(etag, "\""), "\"");
    }

}

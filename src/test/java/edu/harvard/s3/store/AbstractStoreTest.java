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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.stream.Collectors;
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

            assertEquals("93286b7fccf6f6092628ada8b85c0727", normalizeEtag(metsObjectResponse.eTag()));

            String modskey = format("0%1$s/v1/content/metadata/%1$s_mods.xml", id);

            PutObjectRequest modsObjectRequest = PutObjectRequest.builder()
                .bucket(getAwsBucketName())
                .key(modskey)
                .build();

            PutObjectResponse modsObjectResponse = s3.putObject(modsObjectRequest, RequestBody.fromFile(mods));

            assertEquals("93286b7fccf6f6092628ada8b85c0727", normalizeEtag(modsObjectResponse.eTag()));

            String pngkey = format("0%1$s/v1/content/data/%1$s.png", id);

            PutObjectRequest pngObjectRequest = PutObjectRequest.builder()
                .bucket(getAwsBucketName())
                .key(pngkey)
                .build();

            PutObjectResponse pngObjectResponse = s3.putObject(pngObjectRequest, RequestBody.fromFile(png));

            assertEquals("3d8ea40458e95bc4091c5e3460a275bb", normalizeEtag(pngObjectResponse.eTag()));
        }

        /*
        ObjectMapper objectMapper = new ObjectMapper();

        String separator = "%2F";

        String lfsKey = format("0%1$s%2$sv1%2$scontent%2$sdata%2$s%1$s.lfs", ids.get(0), separator);

        Files.createDirectories(Path.of(format("temp/delivery/%s", lfsKey)));

        String lfsFileDataPath = format("temp/delivery/%s/fileData", lfsKey);

        File lfs = createLargeFile(lfsFileDataPath, 5368709121L);

        assertTrue(lfs.exists());


        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

        String creationDate = formatter.format(new Date());
        Date now = new Date();
        String modificationDate = formatter.format(now);

        String nul = null;

        final ObjectNode lfsNode = objectMapper.createObjectNode();
        lfsNode.put("name", lfsKey);
        lfsNode.put("size", lfs.length());
        lfsNode.put("creationDate", creationDate);
        lfsNode.put("modificationDate", modificationDate);
        lfsNode.put("md5", "554157458fc3c9573486e4add4a8fd50");
        lfsNode.put("contentType", "text/plain");
        lfsNode.put("contentEncoding", nul);
        lfsNode.put("kmsEncryption", nul);
        lfsNode.put("lastModified", now.getTime());
        lfsNode.put("dataFile", lfs.getAbsolutePath());
        lfsNode.put("kmsKeyId", nul);
        lfsNode.putObject("userMetadata");
        lfsNode.putArray("tags");
        lfsNode.put("encrypted", false);

        String lfsMetadata = format("temp/delivery/%s/metadata", lfsKey);

        objectMapper.writeValue(new File(lfsMetadata), lfsNode);
        */

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

    String md5(File file) throws NoSuchAlgorithmException, IOException {
        MessageDigest digest = MessageDigest.getInstance("MD5");

        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] byteArray = new byte[1024];
            int bytesCount = 0;
            while ((bytesCount = fis.read(byteArray)) != -1) {
                digest.update(byteArray, 0, bytesCount);
            }
        }

        byte[] bytes = digest.digest();

        StringBuilder result = new StringBuilder();

        for (int i = 0; i < bytes.length; i++) {
            result.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
        }

        return result.toString();
    }

    /*
    private File createLargeFile(final String filename, final long sizeInBytes) throws IOException {
        File file = new File(filename);
        file.createNewFile();

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.setLength(sizeInBytes);
            randomAccessFile.close();

            return file;
        }
    }
    */

    private String normalizeEtag(String etag) {
        return removeEnd(removeStart(etag, "\""), "\"");
    }

}

package org.opennms.influxdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.domain.OnboardingRequest;
import com.influxdb.client.domain.OnboardingResponse;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import okhttp3.OkHttpClient;


/**
 * This test
 * starts a docker container with influxdb
 * writes 5 points
 * loads the measurements and checks
 * if we have 5 measurements.
 */
public class GetMeasurementsTest {

    private String configBucket = "opennms";
    private String configOrg = "opennms";
    private String configUrl = "http://localhost:9999";
    private String configUser = "opennms";
    private String configPassword = "password";

    private Process dockerProcess;
    ExecutorService pool = Executors.newSingleThreadExecutor();

    private InfluxDBClient influxDBClient;

    @Before
    public void setUp() throws IOException, InterruptedException {
        dockerProcess = new ProcessBuilder()
                .command("/usr/bin/bash", "-c", "docker run -p 9999:9999 quay.io/influxdb/influxdb:2.0.0-beta")
                .redirectErrorStream(true)
                .inheritIO()
                .start();

        Thread.sleep(10000); // wait for docker to start // TODO: Patrick: make the waiting more intelligent
        String accessToken = setupInflux();

        InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                .bucket(configBucket)
                .org(configOrg)
                .url(configUrl)
                .authenticateToken(accessToken.toCharArray())
                .build();
        influxDBClient = InfluxDBClientFactory.create(options);
    }

    @After
    public void tearDown() {
        if (influxDBClient != null) {
            influxDBClient.close();
        }
        if (dockerProcess != null && dockerProcess.isAlive()) {
            dockerProcess.destroy();
        }
        pool.shutdown();
    }

    /**
     * Needs a running instance of Influxdb.
     */
    @Test
    public void shouldGetAllMeasurements() throws InterruptedException {
        int noOfMetrics = 5;

        // Add some data
        for (int i = 1; i <= noOfMetrics; i++) {
            addPoint(i);
        }
        influxDBClient.getWriteApi().flush();
        Thread.sleep(2000); // wait for a bit to make sure data was saved.

        // load metrics
        List<String> measurements = loadAllMeasurements();

        // validate
        assertEquals(5, measurements.size());
        for (int i = 1; i <= noOfMetrics; i++) {
            assertTrue(measurements.contains("measurement" + i));
        }

    }

    private List<String> loadAllMeasurements() {
        // TODO: Patrick: The code works but is probably not efficient enough, we should optimize this query since
        // it gets way too much (redundant) data.
        // I am not sure how - the influx documentation doesn't seem to be up to date / correct:
        // https://www.influxdata.com/blog/schema-queries-in-ifql/

        final String query = "from(bucket:\"opennms\")\n" +
                "  |> range(start:-5y)\n" +
                "  |> keys()";

        return this.influxDBClient.getQueryApi()
                .query(query)
                .stream()
                .map(FluxTable::getRecords)
                .flatMap(Collection::stream)
                .map(FluxRecord::getValues)
                .map(m -> m.get("_measurement"))
                .filter(Objects::nonNull)
                .map(Object::toString)
                .distinct()
                .collect(Collectors.toList());
    }

    private void addPoint(int pointId) {
        Point point = Point
                .measurement("measurement" + pointId)
                .addField("value", pointId)
                .time(Instant.now(), WritePrecision.MS)
                .addTag("tag1", "value" + pointId);
        influxDBClient.getWriteApi().writePoint(configBucket, configOrg, point);
    }

    public String setupInflux() {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                .bucket(configBucket)
                .connectionString(configUrl)
                .org(configOrg)
                .url(configUrl)
                .okHttpClient(builder)
                .build();
        InfluxDBClient influxDBClient = InfluxDBClientFactory.create(options);

        System.out.println("Checking preconditions");
        if (!influxDBClient.isOnboardingAllowed()) {
            System.out.println("Onboarding via api is not allowed, please set it up manually. Bye!");
        }
        System.out.println("Preconditions: ok");
        System.out.println(String.format("Create account with user=%s, organization=%s, bucket=%s on url=%s", configUser, configOrg, configBucket, configUrl));
        OnboardingRequest request = new OnboardingRequest()
                .bucket(configBucket)
                .org(configOrg)
                .username(configUser)
                .password(configPassword);
        OnboardingResponse response = influxDBClient.onBoarding(request);
        System.out.println("Create account: ok");
        System.out.println("Access token is: " + response.getAuth().getToken());
        System.out.println("Enjoy!");
        return response.getAuth().getToken();
    }
}

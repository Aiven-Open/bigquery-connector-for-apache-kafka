import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GcpClientBuilderProjectTest {

  private String getProject(GcpClientBuilder<?> builder) throws Exception {
    Field f = GcpClientBuilder.class.getDeclaredField("project");
    f.setAccessible(true);
    return (String) f.get(builder);
  }

  @Test
  public void testProjectFromCredentials() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "config_project");
    properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, GcpClientBuilder.KeySource.JSON.name());
    properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, "{\"project_id\":\"cred_project\"}");
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "dataset");
    properties.put(BigQuerySinkConfig.USE_PROJECT_FROM_CREDENTIALS_CONFIG, "true");
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

    GcpClientBuilder.BigQueryBuilder builder = new GcpClientBuilder.BigQueryBuilder();
    builder.withConfig(config);
    assertEquals("cred_project", getProject(builder));
  }

  @Test
  public void testFallbackToConfigProject() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "config_project");
    properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, GcpClientBuilder.KeySource.JSON.name());
    properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, "{}");
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "dataset");
    properties.put(BigQuerySinkConfig.USE_PROJECT_FROM_CREDENTIALS_CONFIG, "true");
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

    GcpClientBuilder.BigQueryBuilder builder = new GcpClientBuilder.BigQueryBuilder();
    builder.withConfig(config);
    assertEquals("config_project", getProject(builder));
  }
}

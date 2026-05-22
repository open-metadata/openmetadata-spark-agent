package org.openmetadata.transport;

import org.junit.Assert;
import org.junit.Test;

public class OpenMetadataTransportTest {
  @Test
  public void testExtractDbNameFromRedshiftUrl() {
    String result =
        OpenMetadataTransport
            .extractDbNameFromUrl(
                "redshift://localhost:5439/warehouse");
    Assert.assertEquals("warehouse", result);
  }

  @Test
  public void testExtractDbNameFromMysqlUrl() {
    String result =
        OpenMetadataTransport
            .extractDbNameFromUrl(
                "mysql://localhost:3306/experiments?serverTimezone=UTC&rewriteBatchedStatements=true&useSSL=false");
    Assert.assertEquals("experiments", result);
  }
}

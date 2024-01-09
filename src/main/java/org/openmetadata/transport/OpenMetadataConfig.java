/*
 *  Copyright 2024 Collate
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * This code has been referenced from
 * https://github.com/Natural-Intelligence/openLineage-openMetadata-transporter.git
 */

package org.openmetadata.transport;

import io.openlineage.client.transports.TransportConfig;
import java.net.URI;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString
public final class OpenMetadataConfig implements TransportConfig {
  @Getter @Setter private URI hostPort;
  @Getter @Setter private @Nullable Double timeout;
  @Getter @Setter private String pipelineName;
  @Setter private String jwtToken;
  @Getter @Setter private String pipelineSourceUrl;
  @Getter @Setter private String pipelineServiceName;
  @Getter @Setter private String databaseServiceNames;
  @Getter @Setter private @Nullable String pipelineDescription;

  public String getJwtToken() {
    return String.format("Bearer %s", jwtToken);
  }
}

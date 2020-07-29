package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MappingRuleCreator {

  public MappingRulesDescription getMappingRulesFromJson(String jsonPath)
      throws IOException {
    byte[] fileContents = Files.readAllBytes(Paths.get(jsonPath));
    return getMappingRulesFromJson(fileContents);
  }

  MappingRulesDescription getMappingRulesFromJson(byte[] contents)
      throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(contents, MappingRulesDescription.class);
  }

  // TODO
  // methods that process & return list of MappingRules
}

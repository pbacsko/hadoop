package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;

import org.jsonschema2pojo.DefaultGenerationConfig;
import org.jsonschema2pojo.GenerationConfig;
import org.jsonschema2pojo.Jackson2Annotator;
import org.jsonschema2pojo.SchemaGenerator;
import org.jsonschema2pojo.SchemaMapper;
import org.jsonschema2pojo.SchemaStore;
import org.jsonschema2pojo.rules.RuleFactory;

import com.sun.codemodel.JCodeModel;

public class GeneratePojos {
  
  public static void main(String[] args) throws IOException {
    
    JCodeModel codeModel = new JCodeModel();
    URL schemaURL = Paths.get(
        "src/main/resources/MappingRules.json").toUri().toURL();

    GenerationConfig config = new DefaultGenerationConfig() {
      @Override
      public boolean isGenerateBuilders() {
        return false;
      }

      @Override
      public boolean isUsePrimitives() {
          return true;
      }
    };

    SchemaMapper mapper =
        new SchemaMapper(
            new RuleFactory(config,
                new Jackson2Annotator(config),
                new SchemaStore()),
            new SchemaGenerator());

    mapper.generate(codeModel,
        "ignore",
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement",
        schemaURL);

    codeModel.build(new File("src/main/java"));
  }
}

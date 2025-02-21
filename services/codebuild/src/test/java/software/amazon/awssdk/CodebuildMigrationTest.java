/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk;

import static java.util.stream.Collectors.joining;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.codebuild.CodeBuildClient;
import software.amazon.awssdk.services.codebuild.model.BatchGetProjectsResponse;
import software.amazon.awssdk.services.codebuild.model.EnvironmentVariable;
import software.amazon.awssdk.services.codebuild.model.Project;
import software.amazon.awssdk.services.codebuild.model.ProjectEnvironment;

/**
 * 1. get the codebuild environment details
 * 2. remove the 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' from the env var
 * 3. update project with new environment
 */
public class CodebuildMigrationTest {

    CodeBuildClient codebuild = CodeBuildClient.builder()
                                               .region(Region.US_WEST_2)
                                               .build();

    String testProject = "<change-me>";

    @Test
    void get() {
        BatchGetProjectsResponse response = codebuild.batchGetProjects(req -> req.names(testProject));
        Project project = response.projects().get(0);
        System.out.println(project);
        System.out.printf("image : %s%n", project.environment().image());
        System.out.printf("env var: %s%n",
                          project.environment().environmentVariables().stream()
                                 .map(EnvironmentVariable::name)
                                 .collect(joining(", ")));
        List<EnvironmentVariable> envVar = project.environment().environmentVariables();
        List<EnvironmentVariable> filtered = envVar.stream()
                                                   .filter(this::removeAccessKeys)
                                                   .collect(Collectors.toList());
        ProjectEnvironment newEnv = project.environment().toBuilder().environmentVariables(filtered).build();

        codebuild.updateProject(req -> req.environment(newEnv).name(testProject));
    }

    private boolean removeAccessKeys(EnvironmentVariable va) {
        return !"AWS_ACCESS_KEY_ID".equals(va.name()) && !"AWS_SECRET_ACCESS_KEY".equals(va.name());
    }

}

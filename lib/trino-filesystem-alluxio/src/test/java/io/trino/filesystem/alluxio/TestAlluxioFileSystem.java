/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.alluxio;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;

@Testcontainers
public class TestAlluxioFileSystem
        extends AbstractTestAlluxioFileSystem
{
    private static final String IMAGE_NAME = "alluxio/alluxio:2.9.5";
    public static final DockerImageName ALLUXIO_IMAGE = DockerImageName.parse(IMAGE_NAME);

    @Container
    private static final GenericContainer<?> ALLUXIO_MASTER_CONTAINER = createAlluxioMasterContainer();

    @Container
    private static final GenericContainer<?> ALLUXIO_WORKER_CONTAINER = createAlluxioWorkerContainer();

    private static GenericContainer<?> createAlluxioMasterContainer()
    {
        GenericContainer<?> container = new GenericContainer<>(ALLUXIO_IMAGE);
        container.withCommand("master-only")
                .withEnv("ALLUXIO_JAVA_OPTS",
                        "-Dalluxio.security.authentication.type=NOSASL "
                                + "-Dalluxio.master.hostname=localhost "
                                + "-Dalluxio.worker.hostname=localhost "
                                + "-Dalluxio.master.mount.table.root.ufs=/opt/alluxio/underFSStorage "
                                + "-Dalluxio.master.journal.type=NOOP "
                                + "-Dalluxio.security.authorization.permission.enabled=false "
                                + "-Dalluxio.security.authorization.plugins.enabled=false ")
                .withNetworkMode("host")
                .withAccessToHost(true)
                .waitingFor(new LogMessageWaitStrategy()
                        .withRegEx(".*Primary started*\n")
                        .withStartupTimeout(Duration.ofSeconds(180L)));
        return container;
    }

    private static GenericContainer<?> createAlluxioWorkerContainer()
    {
        GenericContainer<?> container = new GenericContainer<>(ALLUXIO_IMAGE);
        container.withCommand("worker-only")
                .withNetworkMode("host")
                .withEnv("ALLUXIO_JAVA_OPTS",
                        "-Dalluxio.security.authentication.type=NOSASL "
                                + "-Dalluxio.worker.ramdisk.size=128MB "
                                + "-Dalluxio.worker.hostname=localhost "
                                + "-Dalluxio.worker.tieredstore.level0.alias=HDD "
                                + "-Dalluxio.worker.tieredstore.level0.dirs.path=/tmp "
                                + "-Dalluxio.master.hostname=localhost "
                                + "-Dalluxio.security.authorization.permission.enabled=false "
                                + "-Dalluxio.security.authorization.plugins.enabled=false ")
                .withAccessToHost(true)
                .dependsOn(ALLUXIO_MASTER_CONTAINER)
                .waitingFor(Wait.forLogMessage(".*Alluxio worker started.*\n", 1));
        return container;
    }

    @BeforeAll
    void setup()
            throws IOException
    {
        initialize();
        // the SSHD container will be stopped by TestContainers on shutdown
        // https://github.com/trinodb/trino/discussions/21969
        System.setProperty("ReportLeakedContainers.disabled", "true");
    }
}

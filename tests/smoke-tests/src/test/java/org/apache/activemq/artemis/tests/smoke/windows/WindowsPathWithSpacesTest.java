/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.smoke.windows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EnabledOnOs(OS.WINDOWS)
public class WindowsPathWithSpacesTest extends SmokeTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final String SERVER_NAME = "windows instance";

   @Test
   public void testArtemisCmdWithOomeDump() throws Exception {
      File instance = getFileServerLocation(SERVER_NAME).getCanonicalFile();
      deleteDirectory(instance);
      runAfter(() -> deleteDirectory(instance));

      helperCreate().setArtemisInstance(instance).createServer();

      // artemis.cmd backs up the heap dump from a previous run before starting the JVM
      File oomeDump = new File(instance, "log\\oom_dump.hprof");
      File oomeDumpBackup = new File(instance, "log\\oom_dump.hprof.bkp");
      assertTrue(oomeDump.createNewFile());

      ProcessBuilder builder = new ProcessBuilder("cmd", "/c", new File(instance, "bin\\artemis.cmd").getPath(), "version");
      builder.environment().remove("ARTEMIS_INSTANCE");
      // only the broker profile defines ARTEMIS_OOME_DUMP
      builder.environment().put("ARTEMIS_PROFILE", "artemis.profile.cmd");
      builder.redirectErrorStream(true);

      Process process = builder.start();
      String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(process.waitFor(1, TimeUnit.MINUTES));
      logger.debug("artemis.cmd output:\n{}", output);

      assertEquals(0, process.exitValue(), output);
      assertTrue(output.contains("Apache Artemis instance: " + instance.getPath()), output);
      assertFalse(output.contains("cannot find"), output);
      assertFalse(output.contains("is not recognized"), output);
      assertFalse(oomeDump.exists(), output);
      assertTrue(oomeDumpBackup.exists(), output);
   }
}

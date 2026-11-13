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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.cli.commands.helper.HelperBase;
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

   private static final String HOME_LINK_NAME = "windows home";

   @Test
   public void testArtemisCmdWithOomeDump() throws Exception {
      File instance = createInstance();

      // artemis.cmd backs up the heap dump from a previous run before starting the JVM
      File oomeDump = new File(instance, "log\\oom_dump.hprof");
      File oomeDumpBackup = new File(instance, "log\\oom_dump.hprof.bkp");
      assertTrue(oomeDump.createNewFile());

      // only the broker profile defines ARTEMIS_OOME_DUMP
      String output = runVersion(new File(instance, "bin\\artemis.cmd"), Map.of("ARTEMIS_PROFILE", "artemis.profile.cmd"));

      assertInstance(output, instance);
      assertFalse(oomeDump.exists(), output);
      assertTrue(oomeDumpBackup.exists(), output);
   }

   @Test
   public void testArtemisCmdWithArtemisInstanceEnv() throws Exception {
      File instance = createInstance();
      File script = new File(instance, "bin\\artemis.cmd");

      assertInstance(runVersion(script, Map.of("ARTEMIS_INSTANCE", instance.getPath())), instance);
      assertInstance(runVersion(script, Map.of("ARTEMIS_INSTANCE", quote(instance.getPath()))), instance);

      String output = runVersion(script, Map.of("ARTEMIS_INSTANCE", instance.getPath() + " missing"));
      assertTrue(output.contains("ARTEMIS_INSTANCE environment variable is set incorrectly"), output);
   }

   @Test
   public void testHomeArtemisCmdWithArtemisHomeEnv() throws Exception {
      File home = createHomeLink();
      File script = new File(home, "bin\\artemis.cmd");

      // without ARTEMIS_HOME the script resolves the home from its own location
      assertHome(runVersion(script, Map.of()), home.getPath() + "\\bin\\..");
      assertHome(runVersion(script, Map.of("ARTEMIS_HOME", home.getPath())), home.getPath());
      assertHome(runVersion(script, Map.of("ARTEMIS_HOME", quote(home.getPath()))), home.getPath());

      String output = runVersion(script, Map.of("ARTEMIS_HOME", home.getPath() + " missing"));
      assertTrue(output.contains("ARTEMIS_HOME environment variable is set incorrectly"), output);
   }

   private File createInstance() throws Exception {
      File instance = getFileServerLocation(SERVER_NAME).getCanonicalFile();
      deleteDirectory(instance);
      runAfter(() -> deleteDirectory(instance));

      helperCreate().setArtemisInstance(instance).createServer();
      return instance;
   }

   /**
    * Links a folder whose name contains a space to the distribution, so the home script runs from a path with spaces
    * without copying the distribution. A directory junction does not require administrator rights.
    */
   private File createHomeLink() throws Exception {
      File distribution = HelperBase.getHome(ARTEMIS_HOME_PROPERTY).getCanonicalFile();
      File link = getFileServerLocation(HOME_LINK_NAME).getCanonicalFile();
      removeHomeLink(link);
      runAfter(() -> removeHomeLink(link));

      assertTrue(link.getParentFile().isDirectory() || link.getParentFile().mkdirs());
      Process process = new ProcessBuilder("cmd", "/c", "mklink", "/J", link.getPath(), distribution.getPath()).redirectErrorStream(true).start();
      String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(process.waitFor(1, TimeUnit.MINUTES));
      assertEquals(0, process.exitValue(), output);
      return link;
   }

   private static void removeHomeLink(File link) throws Exception {
      if (link.exists()) {
         // rmdir without /s removes only the junction, never the content of the distribution it points to
         Process process = new ProcessBuilder("cmd", "/c", "rmdir", link.getPath()).redirectErrorStream(true).start();
         process.getInputStream().readAllBytes();
         assertTrue(process.waitFor(1, TimeUnit.MINUTES));
      }
   }

   private static String runVersion(File script, Map<String, String> environment) throws Exception {
      ProcessBuilder builder = new ProcessBuilder("cmd", "/c", script.getPath(), "version");
      builder.environment().remove("ARTEMIS_HOME");
      builder.environment().remove("ARTEMIS_INSTANCE");
      builder.environment().remove("ARTEMIS_PROFILE");
      builder.environment().putAll(environment);
      builder.redirectErrorStream(true);

      Process process = builder.start();
      String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(process.waitFor(1, TimeUnit.MINUTES));
      logger.debug("{} {} output:\n{}", script, environment, output);

      assertEquals(0, process.exitValue(), output);
      assertFalse(output.contains("cannot find"), output);
      assertFalse(output.contains("is not recognized"), output);
      assertFalse(output.contains("was unexpected at this time"), output);
      return output;
   }

   private static void assertInstance(String output, File instance) {
      assertTrue(output.contains("Apache Artemis instance: " + instance.getPath()), output);
   }

   private static void assertHome(String output, String home) {
      assertTrue(output.contains("Apache Artemis home: " + home), output);
   }

   private static String quote(String value) {
      return "\"" + value + "\"";
   }
}

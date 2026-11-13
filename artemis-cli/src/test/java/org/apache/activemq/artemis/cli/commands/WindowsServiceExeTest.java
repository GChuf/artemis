/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.cli.commands;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.cli.test.CliTestBase;
import org.apache.activemq.cli.test.TestActionContext;
import org.junit.jupiter.api.Test;

/**
 * The Windows service wrapper is shipped in {@code bin/windows} of the distribution instead of the artemis-cli jar.
 */
public class WindowsServiceExeTest extends CliTestBase {

   private static final byte[] OLD_EXE = "old-service-exe".getBytes(StandardCharsets.UTF_8);
   private static final byte[] NEW_EXE = "new-service-exe".getBytes(StandardCharsets.UTF_8);

   @Test
   public void testServiceExeNotBundledInJar() {
      assertNull(Create.class.getResource(Create.BIN_ARTEMIS_SERVICE_EXE));
   }

   @Test
   public void testCreateCopiesServiceExeFromHome() throws Exception {
      File home = createHome("home", OLD_EXE);
      File instance = new File(temporaryFolder, "instance");

      TestActionContext context = new TestActionContext();
      create(home, instance).execute(context);

      assertArrayEquals(OLD_EXE, Files.readAllBytes(new File(instance, Create.BIN_ARTEMIS_SERVICE_EXE).toPath()));
      assertTrue(context.getStdout().contains(Create.ARTEMIS_SERVICE_EXE + "\" install"), context.getStdout());
   }

   @Test
   public void testCreateWithoutServiceExeInHome() throws Exception {
      File home = createHome("home", null);
      File instance = new File(temporaryFolder, "instance");

      TestActionContext context = new TestActionContext();
      create(home, instance).execute(context);

      assertFalse(new File(instance, Create.BIN_ARTEMIS_SERVICE_EXE).exists());
      // the rest of the Windows scripts are still created
      assertTrue(new File(instance, Create.BIN_ARTEMIS_CMD).exists());
      assertTrue(new File(instance, Create.BIN_ARTEMIS_SERVICE_XML).exists());
      assertEquals("", context.getStderr());
      assertFalse(context.getStdout().contains(Create.ARTEMIS_SERVICE_EXE + "\" install"), context.getStdout());
   }

   @Test
   public void testCreateNixHasNoWindowsFiles() throws Exception {
      File home = createHome("home", OLD_EXE);
      File instance = new File(temporaryFolder, "instance");

      Create create = create(home, instance);
      create.setWindows(false);
      create.setNix(true);
      create.execute(new TestActionContext());

      assertTrue(new File(instance, Create.BIN_ARTEMIS).exists());
      assertFalse(new File(instance, "bin/windows").exists());
      assertFalse(new File(instance, Create.BIN_ARTEMIS_SERVICE_EXE).exists());
      assertFalse(new File(instance, Create.BIN_ARTEMIS_CMD).exists());
      assertFalse(new File(instance, Create.BIN_ARTEMIS_SERVICE_XML).exists());
   }

   @Test
   public void testUpgradeReplacesServiceExeFromHome() throws Exception {
      File oldHome = createHome("old-home", OLD_EXE);
      File newHome = createHome("new-home", NEW_EXE);
      File instance = new File(temporaryFolder, "instance");
      create(oldHome, instance).execute(new TestActionContext());

      upgrade(newHome, instance).run(new TestActionContext());

      assertArrayEquals(NEW_EXE, Files.readAllBytes(new File(instance, Create.BIN_ARTEMIS_SERVICE_EXE).toPath()));
      assertArrayEquals(OLD_EXE, Files.readAllBytes(new File(instance, "old-config-bkp.0/" + Create.BIN_ARTEMIS_SERVICE_EXE).toPath()));
   }

   @Test
   public void testUpgradeKeepsServiceExeWhenMissingInHome() throws Exception {
      File oldHome = createHome("old-home", OLD_EXE);
      File newHome = createHome("new-home", null);
      File instance = new File(temporaryFolder, "instance");
      create(oldHome, instance).execute(new TestActionContext());

      TestActionContext context = new TestActionContext();
      upgrade(newHome, instance).run(context);

      assertArrayEquals(OLD_EXE, Files.readAllBytes(new File(instance, Create.BIN_ARTEMIS_SERVICE_EXE).toPath()));
      assertEquals("", context.getStderr());
   }

   @Test
   public void testUpgradeInstanceWithoutServiceExe() throws Exception {
      File oldHome = createHome("old-home", null);
      File newHome = createHome("new-home", NEW_EXE);
      File instance = new File(temporaryFolder, "instance");
      create(oldHome, instance).execute(new TestActionContext());
      assertFalse(new File(instance, Create.BIN_ARTEMIS_SERVICE_EXE).exists());

      // nothing to back up, the service wrapper of the new home is copied
      upgrade(newHome, instance).run(new TestActionContext());
      assertArrayEquals(NEW_EXE, Files.readAllBytes(new File(instance, Create.BIN_ARTEMIS_SERVICE_EXE).toPath()));
      assertFalse(new File(instance, "old-config-bkp.0/" + Create.BIN_ARTEMIS_SERVICE_EXE).exists());

      // neither the instance nor the home have a service wrapper
      Files.delete(new File(instance, Create.BIN_ARTEMIS_SERVICE_EXE).toPath());
      TestActionContext context = new TestActionContext();
      upgrade(oldHome, instance).run(context);
      assertFalse(new File(instance, Create.BIN_ARTEMIS_SERVICE_EXE).exists());
      assertEquals("", context.getStderr());
   }

   private File createHome(String name, byte[] serviceExe) throws Exception {
      File home = new File(temporaryFolder, name);
      File exe = new File(home, Create.HOME_BIN_WINDOWS_ARTEMIS_SERVICE_EXE);
      Files.createDirectories(exe.getParentFile().toPath());
      if (serviceExe != null) {
         Files.write(exe.toPath(), serviceExe);
      }
      return home;
   }

   private Create create(File home, File instance) {
      Create create = new Create();
      create.setHomeValues(home, null, null);
      create.setWindows(true);
      create.setNoAutoTune(true);
      create.setInstance(instance);
      create.setUser("admin");
      create.setPassword(RandomUtil.randomUUIDString());
      return create;
   }

   private Upgrade upgrade(File home, File instance) {
      Upgrade upgrade = new Upgrade();
      upgrade.setHomeValues(home, null, null);
      upgrade.setWindows(true);
      upgrade.setDirectory(instance);
      return upgrade;
   }
}

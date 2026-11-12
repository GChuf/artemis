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
package org.apache.activemq.cli.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;

import org.apache.activemq.artemis.cli.commands.Create;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class WindowsServiceXmlTest {

   /**
    * WinSW wraps every argument containing spaces in quotes. An argument that is already partially quoted (e.g.
    * {@code -Dfoo="%VAR%"}) ends up as {@code "-Dfoo="C:\a b""} which the JVM splits into two arguments when the
    * expanded path contains a space.
    */
   @Test
   public void testArgumentsWithVariablesAreNotQuoted() throws Exception {
      Document document;
      try (InputStream stream = Create.class.getResourceAsStream(Create.BIN_ARTEMIS_SERVICE_XML)) {
         assertNotNull(stream);
         document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(stream);
      }

      int checked = 0;
      for (String tag : new String[]{"startargument", "stopargument"}) {
         NodeList arguments = document.getElementsByTagName(tag);
         for (int i = 0; i < arguments.getLength(); i++) {
            String argument = arguments.item(i).getTextContent();
            if (argument.contains("%")) {
               assertFalse(argument.contains("\""), "<" + tag + "> must not be quoted: " + argument);
               checked++;
            }
         }
      }
      assertTrue(checked > 0);
   }
}

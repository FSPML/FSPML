/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Adds Hama configuration files to a Configuration.
 */
public class HamaConfiguration extends Configuration {
  /** constructor */
  public HamaConfiguration() {
    super();
    addHamaResources();
  }

  public HamaConfiguration(Path confFile) {
    super();
    this.addResource(confFile);
  }
  
  /**
   * Create a clone of passed configuration.
   * 
   * @param c Configuration to clone.
   */
  public HamaConfiguration(final Configuration c) {
    this();
    for (Entry<String, String> e : c) {
      set(e.getKey(), e.getValue());
    }
  }

  /**
   * Adds Hama configuration files to a Configuration
   */
  private void addHamaResources() {
    Configuration.addDefaultResource("termite-default.xml");
    Configuration.addDefaultResource("termite-site.xml");
  }
}

/**
 * Copyright (c) Codice Foundation
 *
 * <p>This is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or any later version.
 *
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details. A copy of the GNU Lesser General Public
 * License is distributed along with this program and can be found at
 * <http://www.gnu.org/licenses/lgpl.html>.
 */
package ddf.test.itests.catalog;

import org.codice.ddf.test.common.LoggingUtils;
import org.codice.ddf.test.common.annotations.BeforeExam;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

/** Tests the Catalog framework components. Includes helper methods at the Catalog level. */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
public class TestMuxCatalog extends TestCatalog {

  @BeforeExam
  @Override
  public void beforeExam() {
    try {
      waitForSystemReady();
      getServiceManager().startFeature(true, "catalog-mux-solr-provider");
      getServiceManager().waitForAllBundles();
    } catch (Exception e) {
      LoggingUtils.failWithThrowableStacktrace(e, "Failed in @BeforeExam: ");
    }
  }
}

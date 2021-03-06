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
package org.codice.ddf.security.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import ddf.security.Subject;
import ddf.security.service.SecurityManager;
import ddf.security.service.SecurityServiceException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;
import java.util.concurrent.Callable;
import org.apache.shiro.subject.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;

public class SecurityTest {

  private org.codice.ddf.security.impl.Security security;

  @Mock private org.apache.shiro.subject.Subject shiroSubject;

  @Mock private Subject systemSubject;

  @Mock private Callable<String> callable;

  @Before
  public void setup() throws URISyntaxException {
    System.setProperty("karaf.local.roles", "admin,local");
    security = new Security();

    initMocks(this);

    when(shiroSubject.execute(callable)).thenReturn("Success!");

    System.setProperty("javax.net.ssl.keyStoreType", "JKS");
    System.setProperty(
        "javax.net.ssl.keyStore", getClass().getResource("/secureKeystore.jks").toURI().getPath());
    System.setProperty("javax.net.ssl.keyStorePassword", "password");
    System.setProperty("ddf.home", "/ddf/home");
    System.setProperty("org.codice.ddf.system.hostname", "localhost");

    security = spy(security);
  }

  @Test
  public void testGetSubjectNoSecurityManager() throws Exception {
    configureMockForSecurityManager(null);
    Subject subject = security.getSubject("username", "password", "127.0.0.1");
    assertThat(subject, is(equalTo(null)));
  }

  @Test
  public void testGetSubjectInvalidUsernamePassword() throws Exception {
    SecurityManager sm = mock(SecurityManager.class);
    when(sm.getSubject(Matchers.any())).thenThrow(new SecurityServiceException("Error"));

    configureMockForSecurityManager(sm);

    Subject subject = security.getSubject("username", "password", "127.0.0.1");
    assertThat(subject, is(equalTo(null)));
  }

  @Test
  public void testGetSubject() throws Exception {
    SecurityManager sm = mock(SecurityManager.class);
    Subject smSubject = mock(Subject.class);
    when(sm.getSubject(Matchers.any())).thenReturn(smSubject);

    configureMockForSecurityManager(sm);

    Subject subject = security.getSubject("username", "password", "127.0.0.1");
    assertThat(subject, not(equalTo(null)));
  }

  @Test
  public void testJavaSubjectDoesNotHaveAdminRole() throws Exception {
    javax.security.auth.Subject subject = new javax.security.auth.Subject();
    javax.security.auth.Subject.doAs(
        subject,
        (PrivilegedAction<Object>)
            () -> {
              assertThat(security.javaSubjectHasAdminRole(), equalTo(false));
              return null;
            });
  }

  @Test
  public void testJavaSubjectHasAdminRole() throws Exception {
    security.runAsAdmin(
        () -> {
          assertThat(security.javaSubjectHasAdminRole(), equalTo(true));
          return null;
        });
  }

  @Test
  public void testGetSystemSubject() throws Exception {
    configureMocksForBundleContext("server");

    security.runAsAdmin(
        () -> {
          assertThat(security.getSystemSubject(), not(equalTo(null)));
          return null;
        });

    verify(security).auditSystemSubjectAccess();
  }

  @Test
  public void testGetSystemSubjectBadAlias() throws Exception {
    configureMocksForBundleContext("bad-alias");

    assertThat(security.getSystemSubject(), equalTo(null));
  }

  @Test
  public void testRunWithSubjectOrElevateWhenUserSubjectExists() throws Exception {
    doReturn(shiroSubject).when(security).getShiroSubject();

    String result = security.runWithSubjectOrElevate(callable);

    assertThat(result, is("Success!"));
    verify(shiroSubject).execute(callable);
  }

  @Test
  public void testRunWithSubjectOrElevateWhenSystemSubjectHasAdminRole() throws Exception {
    doReturn(null).when(security).getShiroSubject();
    when(systemSubject.execute(callable)).thenReturn("Success!");
    configureMocksForBundleContext("server");

    String result =
        security.runAsAdminWithException(() -> security.runWithSubjectOrElevate(callable));

    assertThat(result, is("Success!"));
    verify(security).auditSystemSubjectElevation();
    verifyZeroInteractions(shiroSubject);
  }

  @Test(expected = SecurityServiceException.class)
  public void testRunWithSubjectOrElevateWhenSystemSubjectDoesNotHaveAdminRole() throws Exception {
    doReturn(null).when(security).getShiroSubject();
    when(systemSubject.execute(callable)).thenReturn("Success!");
    configureMocksForBundleContext("server");

    try {
      security.runWithSubjectOrElevate(callable);
    } catch (SecurityServiceException e) {
      verify(security).auditInsufficientPermissions();
      throw e;
    }
  }

  @Test
  public void testRunWithSubjectOrElevateWhenSystemSubjectIsNotAvailable() throws Exception {
    doReturn(null).when(security).getShiroSubject();
    configureMocksForBundleContext("bad-alias");

    boolean securityExceptionThrown =
        security.runAsAdmin(
            () -> {
              try {
                return security.runWithSubjectOrElevate(() -> false);
              } catch (SecurityServiceException e) {
                return true;
              } catch (Exception e) {
                return false;
              }
            });

    assertThat(securityExceptionThrown, is(true));
    verify(security).auditInsufficientPermissions();
    verifyZeroInteractions(shiroSubject);
  }

  @Test
  public void testRunWithSubjectOrElevateWhenUserSubjectExistsAndCallableThrowsException()
      throws Exception {
    doReturn(shiroSubject).when(security).getShiroSubject();
    when(shiroSubject.execute(callable))
        .thenThrow(new ExecutionException(new UnsupportedOperationException()));

    try {
      security.runWithSubjectOrElevate(callable);
      fail("InvocationTargetException expected");
    } catch (SecurityServiceException e) {
      throw e;
    } catch (InvocationTargetException e) {
      assertThat(e.getCause(), is(instanceOf(UnsupportedOperationException.class)));
    }

    verify(security)
        .auditFailedCodeExecutionForSystemSubject(Matchers.any(ExecutionException.class));
  }

  @Test
  public void testRunWithSubjectOrElevateWhenSystemSubjectIsUsedAndCallableThrowsException()
      throws Exception {
    doReturn(null).when(security).getShiroSubject();
    when(systemSubject.execute(callable))
        .thenThrow(new ExecutionException(new UnsupportedOperationException()));
    configureMocksForBundleContext("server");

    Exception exception =
        security.runAsAdmin(
            () -> {
              try {
                security.runWithSubjectOrElevate(callable);
                fail("InvocationTargetException expected");
                return null;
              } catch (Exception e) {
                return e;
              }
            });

    assertThat(exception, is(instanceOf(InvocationTargetException.class)));
    assertThat(exception.getCause(), is(instanceOf(UnsupportedOperationException.class)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRunWithSubjectOrElevateWhenCallableIsNull() throws Exception {
    security.runWithSubjectOrElevate(null);
  }

  private void configureMockForSecurityManager(SecurityManager sm) {
    BundleContext bc = mock(BundleContext.class);
    doReturn(bc).when(security).getBundleContext();
    ServiceReference ref = mock(ServiceReference.class);
    when(bc.getServiceReference(Matchers.any(Class.class))).thenReturn(ref);
    when(bc.getService(ref)).thenReturn(sm);
  }

  private void configureMocksForBundleContext(String systemHostname) throws Exception {
    System.setProperty("org.codice.ddf.system.hostname", systemHostname);
    BundleContext bundleContext = mock(BundleContext.class);
    doReturn(bundleContext).when(security).getBundleContext();
    ServiceReference adminRef = mock(ServiceReference.class);
    ConfigurationAdmin configAdmin = mock(ConfigurationAdmin.class);
    Configuration config = mock(Configuration.class);
    when(configAdmin.getConfiguration(Matchers.anyString(), Matchers.anyString()))
        .thenReturn(config);
    when(bundleContext.getService(adminRef)).thenReturn(configAdmin);
    ServiceReference securityRef = mock(ServiceReference.class);
    SecurityManager securityManager = mock(SecurityManager.class);
    when(securityManager.getSubject(Matchers.any())).thenReturn(systemSubject);
    when(bundleContext.getService(securityRef)).thenReturn(securityManager);
    when(bundleContext.getServiceReference(ConfigurationAdmin.class)).thenReturn(adminRef);
    when(bundleContext.getServiceReference(SecurityManager.class)).thenReturn(securityRef);
  }
}

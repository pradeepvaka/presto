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
package com.facebook.presto.router.scheduler;

import com.google.inject.Binder;
import com.google.inject.Module;

import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.Scopes.SINGLETON;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class PlanCheckerRouterPluginModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(PlanCheckerRouterPluginConfig.class);
        binder.bind(PlanCheckerRouterPluginPrestoClient.class).in(SINGLETON);
        binder.bind(PlanCheckerRouterPluginScheduler.class).in(SINGLETON);

        newExporter(binder).export(PlanCheckerRouterPluginPrestoClient.class).withGeneratedName();
        binder.bind(MBeanServer.class).toInstance(ManagementFactory.getPlatformMBeanServer());
    }
}

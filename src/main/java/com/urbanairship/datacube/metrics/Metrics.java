package com.urbanairship.datacube.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * A Singleton for a MetricsRegistry. Use of this mechanism is not strictly necessary
 * in application space. It's here for consistency within this library and as a
 * convenience but applications may create simple instances of {@link MetricRegistry}
 * objects at their whim.
 *
 * This class also exposes simple pass through statics for static imports against a single
 * class if desired.
 */
public final class Metrics {

    private Metrics() {
        //no instances
    }

    private static final MetricRegistry registry = new MetricRegistry();

    //UA default is to use MS for durations and rates so we configure that here
    //The reporter can be accessed or the registry can be reported on using
    //another

    private static final JmxReporter reporter = JmxReporter.forRegistry(registry)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .convertRatesTo(TimeUnit.SECONDS)
            .createsObjectNamesWith(new ObjectNameFactoryImpl())
            .build();

    static {
        reporter.start();
    }

    public static MetricRegistry getRegistry() {
        return registry;
    }

    public static JmxReporter getReporter() {
        return reporter;
    }

    public static MetricNameDetails name(Class clazz, String name) {
        return MetricNameDetails.newBuilder()
                .setGroup(clazz.getPackage() == null ? "" : clazz.getPackage().getName())
                .setType(clazz.getSimpleName().replaceAll("\\$$", ""))
                .setName(name)
                .build();
    }

    public static MetricNameDetails name(Class clazz, String name, String scope) {
        return MetricNameDetails.newBuilder()
                .setGroup(clazz.getPackage() == null ? "" : clazz.getPackage().getName())
                .setType(clazz.getSimpleName().replaceAll("\\$$", ""))
                .setName(name)
                .setScope(scope)
                .build();
    }

    public static MetricNameDetails name(String group, String type, String name) {
        return MetricNameDetails.newBuilder()
                .setGroup(group)
                .setType(type)
                .setName(name)
                .build();
    }

    public static Meter meter(MetricNameDetails nameDetails) {
        return registry.meter(nameDetails.getFormattedJmxName());
    }

    public static Meter meter(Class clazz, String name) {
        return meter(name(clazz, name));
    }

    public static Meter meter(Class clazz, String name, String scope) {
        return meter(name(clazz, name, scope));
    }

    public static Counter counter(MetricNameDetails nameDetails) {
        return registry.counter(nameDetails.getFormattedJmxName());
    }

    public static Counter counter(Class clazz, String name) {
        return counter(name(clazz, name));
    }

    public static Counter counter(Class clazz, String name, String scope) {
        return counter(name(clazz, name, scope));
    }

    public static Histogram histogram(MetricNameDetails nameDetails) {
        return registry.histogram(nameDetails.getFormattedJmxName());
    }

    public static Histogram histogram(Class clazz, String name) {
        return histogram(name(clazz, name));
    }

    public static Histogram histogram(Class clazz, String name, String scope) {
        return histogram(name(clazz, name, scope));
    }

    public static Timer timer(MetricNameDetails nameDetails) {
        return registry.timer(nameDetails.getFormattedJmxName());
    }

    public static Timer timer(Class clazz, String name) {
        return timer(name(clazz, name));
    }

    public static Timer timer(Class clazz, String name, String scope) {
        return timer(name(clazz, name, scope));
    }

    public static <T> void gauge(MetricNameDetails nameDetails, Gauge<T> impl) {
        final String newGaugeName = nameDetails.getFormattedJmxName();
        SortedMap<String, Gauge> found = registry.getGauges(new MetricFilter() {
            @Override
            public boolean matches(String name, Metric metric) {
                return newGaugeName.equals(name);
            }
        });

        if (found.isEmpty()) {
            registry.register(newGaugeName, impl);
        }
    }

    /**
     * This doesn't make a ton of sense given the above but it will
     * likely be cleaner in Java8. Also avoids having to catch an exception
     * in a Gauge implementation.
     */
    public static <T> void gauge(MetricNameDetails nameDetails, final Callable<T> impl) {
        gauge(nameDetails, new Gauge<T>() {
            @Override
            public T getValue() {
                try {
                    return impl.call();
                }
                catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
    }

    public static <T> void gauge(Class clazz, String name, Gauge<T> impl) {
        gauge(name(clazz, name), impl);
    }

    public static <T> void gauge(Class clazz, String name, String scope, Gauge<T> impl) {
        gauge(name(clazz, name, scope), impl);
    }
}

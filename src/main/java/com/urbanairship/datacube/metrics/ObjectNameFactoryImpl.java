package com.urbanairship.datacube.metrics;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.ObjectNameFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;


public class ObjectNameFactoryImpl implements ObjectNameFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmxReporter.class);

    @Override
    public ObjectName createName(String type, String domain, String name) {
        try {
            return new ObjectName(name);
        } catch (MalformedObjectNameException e) {
            try {
                return new ObjectName(ObjectName.quote(name));
            } catch (MalformedObjectNameException e1) {
                try {
                    // fall back to the Metrics 3 naming
                    return createMetrics3Name(domain, name);
                } catch (MalformedObjectNameException e2) {
                    LOGGER.warn("Unable to register " + name, e2);
                    throw new RuntimeException(e2);
                }
            }
        }
    }

    // Default behavior of Metrics 3 library, for fallback
    private ObjectName createMetrics3Name(String domain, String name) throws MalformedObjectNameException {
        try {
            return new ObjectName(domain, "name", name);
        } catch (MalformedObjectNameException e) {
            return new ObjectName(domain, "name", ObjectName.quote(name));
        }
    }
}

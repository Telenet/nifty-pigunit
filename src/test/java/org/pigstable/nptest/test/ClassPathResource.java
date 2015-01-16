package org.pigstable.nptest.test;

import java.net.URL;

/**
 * Small utility class to easily find resources on the system's classpath.
 *
 * @author mlavaert
 */
public final class ClassPathResource {

    private final String resourcePath;

    private ClassPathResource(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    public static ClassPathResource create(String location) {
        return new ClassPathResource(location);
    }

    public final String systemPath() {
        URL resource = getClassLoader().getResource(resourcePath);
        if (resource != null) {
            return resource.getFile();
        } throw new IllegalArgumentException("The given file: " + resourcePath + " could not be retrieved from the classpath");
    }

    private static ClassLoader getClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }
}
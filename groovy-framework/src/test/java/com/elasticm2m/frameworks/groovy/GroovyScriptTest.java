package com.elasticm2m.frameworks.groovy;

import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class GroovyScriptTest extends Assert {

    @Test
    public void testSimpleScript() throws Exception {

        String script = "return body";
        String body = "{ \"id\": \"test\", \"type\": \"Feature\", \"properties\": {}}";

        GroovyShell shell = new GroovyShell();
        Script compiledScript = shell.parse(new File("src/test/resources/transform_json.groovy"));
        compiledScript.setProperty("body", body);
        Object result = compiledScript.run();
        assertNotNull(result);
    }

}

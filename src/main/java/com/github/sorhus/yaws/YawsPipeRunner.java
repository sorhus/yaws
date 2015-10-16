package com.github.sorhus.yaws;

import com.github.sorhus.yaws.model.YawsPipe;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class YawsPipeRunner {

    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {

        String clazz = args[0];
        String pipeName = args[1];
        List<String> pipeArgs = Collections.unmodifiableList(
                Arrays.asList(Arrays.copyOfRange(args, 2, args.length)));

        Constructor<?> pipeConstructor = Class.forName(clazz).getConstructor(String.class, List.class);
        YawsPipe pipe = (YawsPipe) pipeConstructor.newInstance(pipeName, pipeArgs);

        pipe.run();
        System.out.println(pipe);
    }
}

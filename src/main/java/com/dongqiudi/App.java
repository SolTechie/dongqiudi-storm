package com.dongqiudi;


import com.dongqiudi.topologies.NginxLogTopology;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class App {

    /**
     * args e.g.
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        String mode = "cluster";
        if (0 == args.length) {
            mode = "local";
            properties.load(ClassLoader.getSystemClassLoader().getResourceAsStream("task.properties"));
        } else {
            properties.load(new FileInputStream(args[0]));
        }
        Topology topology = new NginxLogTopology(properties);
        topology.submit(mode);


    }
}

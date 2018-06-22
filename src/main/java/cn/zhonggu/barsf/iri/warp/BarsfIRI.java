package cn.zhonggu.barsf.iri.warp;

import cn.zhonggu.barsf.iri.runner.TransactionAnalysisRunner;
import com.iota.iri.IXI;
import com.iota.iri.Iota;
import com.iota.iri.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Created by ZhuDH on 2018/5/4.
 */
public class BarsfIRI extends com.iota.iri.IRI {
    private static final Logger log = LoggerFactory.getLogger(BarsfIRI.class);
    public static IotaWrapper iota;
    public static APIWrapper api;
    public static IXI ixi;
    public static Configuration configuration;

    public static void main(String[] args) throws IOException {
        configureLogging();

        configuration = new Configuration();
        if (!validateParams(configuration, args)) {
            printUsage();
            return;
        }

        log.info("Welcome to {} {}", configuration.booling(Configuration.DefaultConfSettings.TESTNET) ? TESTNET_NAME : MAINNET_NAME, VERSION);
        iota = new IotaWrapper(configuration);
        ixi = new IXI(iota);
        api = new APIWrapper(iota, ixi);
        shutdownHook();

        if (configuration.booling(Configuration.DefaultConfSettings.DEBUG)) {
            log.info("You have set the debug flag. To enable debug output, you need to uncomment the DEBUG appender in the source tree at iri/src/main/resources/logback.xml and re-package iri.jar");
        }

        if (configuration.booling(Configuration.DefaultConfSettings.EXPORT)) {
            File exportDir = new File("export");
            // if the directory does not exist, create it
            if (!exportDir.exists()) {
                log.info("Create directory 'export'");
                try {
                    exportDir.mkdir();
                } catch (SecurityException e) {
                    log.error("Could not create directory", e);
                }
            }
            exportDir = new File("export-solid");
            // if the directory does not exist, create it
            if (!exportDir.exists()) {
                log.info("Create directory 'export-solid'");
                try {
                    exportDir.mkdir();
                } catch (SecurityException e) {
                    log.error("Could not create directory", e);
                }
            }
        }

        try {
            iota.init();
            api.init();
            ixi.init(configuration.string(Configuration.DefaultConfSettings.IXI_DIR));
        } catch (final Exception e) {
            log.error("Exception during IOTA node initialisation: ", e);
            e.printStackTrace();
            System.exit(-1);
        }
        log.info("IOTA Node initialised correctly.");
        // =============================== iota 节点启动完毕 ======================================
        // 自启动任务
        TransactionAnalysisRunner.selfCall();
    }

    private static void shutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down IOTA node, please hold tight...");

            try {
                ixi.shutdown();
                api.shutDown();
                iota.shutdown();
            } catch (Exception var1) {
                log.error("Exception occurred shutting down IOTA node: ", var1);
            }

        }, "Shutdown Hook"));
    }

    private static void printUsage() {
        log.info("Usage: java -jar {}-{}.jar " +
                        "[{-n,--neighbors} '<list of neighbors>'] " +
                        "[{-p,--port} 14265] " +
                        "[{-c,--config} 'config-file-name'] " +
                        "[{-u,--udp-receiver-port} 14600] " +
                        "[{-t,--tcp-receiver-port} 15600] " +
                        "[{-d,--debug} false] " +
                        "[{--testnet} false]" +
                        "[{--remote} false]" +
                        "[{--remote-auth} string]" +
                        "[{--remote-limit-api} string]"
                , MAINNET_NAME, VERSION);
    }

    private static void configureLogging() {
        String config = System.getProperty("logback.configurationFile");
        String level = System.getProperty("logging-level", "").toUpperCase();
        switch (level) {
            case "OFF":
            case "ERROR":
            case "WARN":
            case "INFO":
            case "DEBUG":
            case "TRACE":
                break;
            case "ALL":
                level = "TRACE";
                break;
            default:
                level = "INFO";
        }
        System.getProperties().put("logging-level", level);
        System.out.println("Logging - property 'logging-level' set to: [" + level + "]");
        if (config != null) {
            System.out.println("Logging - alternate logging configuration file specified at: '" + config + "'");
        }
    }
}

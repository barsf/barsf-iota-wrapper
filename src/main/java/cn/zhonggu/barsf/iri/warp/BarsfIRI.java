package cn.zhonggu.barsf.iri.warp;

import cn.zhonggu.barsf.iri.analysis.TransactionAnalysisRunner;
import com.iota.iri.IXI;
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

    public static void main(String[] args) throws IOException {
        configuration = new Configuration();
        validateParams(configuration, args);

        log.info("Welcome to {} {}", configuration.booling(Configuration.DefaultConfSettings.TESTNET) ? TESTNET_NAME : MAINNET_NAME, VERSION);
        iota = new IotaWrapper(configuration);
        ixi = new IXI(iota);
        APIWrapper apiWrapper = new APIWrapper(iota, ixi);
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
            apiWrapper.init();
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
}

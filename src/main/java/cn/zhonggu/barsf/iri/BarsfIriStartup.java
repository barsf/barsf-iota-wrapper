package cn.zhonggu.barsf.iri;


import cn.zhonggu.barsf.iri.runner.RodbDataFishingRunner;
import cn.zhonggu.barsf.iri.warp.BarsfIRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static cn.zhonggu.barsf.iri.runner.RodbDataFishingRunner.synced;

/**
 * Created by ZhuDH on 2018/5/4.
 * 主启动类
 */
public class BarsfIriStartup {
    private static final Logger log = LoggerFactory.getLogger(BarsfIriStartup.class);
    private static final String WIPE_AND_LOAD_PARAM = "--reload";

    // ================== 启动参数 ===================
    public static boolean wipeAndLoadFromRdb = false;

    public static void main(String[] args) throws IOException {
        args = parseParams(args);
        WipeAndLoadFromRDB();

        log.info("Barsf-iri the iri wrapper started.");
        BarsfIRI.main(args);


    }

    /* 解析并去掉不属于iota-iri的jar包 */
    private static String[] parseParams(String[] args) {
        List<String> params = new ArrayList<>(Arrays.asList(args));
        Iterator<String> iter = params.iterator();
        while (iter.hasNext()) {
            if (iter.next().equals(WIPE_AND_LOAD_PARAM)) {
                iter.remove();
                wipeAndLoadFromRdb = true;
            }
        }
        // iota-iri 不支持未定义的参数,这里去掉
        return params.toArray(new String[0]);
    }

    private static void WipeAndLoadFromRDB() {
        log.info(" wipe and load from RDB");

        try {
            if (wipeAndLoadFromRdb) {
                RodbDataFishingRunner.selfCall();
            } else {
                synced = true;
            }

            while (!synced) {
                Thread.sleep(20000);
            }
        } catch (Exception e) {
            log.error("WipeAndLoadFromRDB failed", e);
        }
    }
}

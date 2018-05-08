package cn.zhonggu.barsf.iri;


import cn.zhonggu.barsf.iri.warp.BarsfIRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ZhuDH on 2018/5/4.
 * 主启动类
 */
public class BarsfIriStartup {
    private static final Logger log = LoggerFactory.getLogger(BarsfIriStartup.class);
    public static void main(String[] args) throws IOException {
        log.info("Barsf-iri the iri wrapper started.");
        BarsfIRI.main(args);
    }
}

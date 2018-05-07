package cn.zhonggu.barsf.iri.warp;

import cn.zhonggu.barsf.iri.storage.innoDB.InnoDBPersistenceProvider;
import com.iota.iri.conf.Configuration;
import com.iota.iri.hash.SpongeFactory;
import com.iota.iri.model.StateDiff;
import com.iota.iri.model.Transaction;
import com.iota.iri.storage.FileExportProvider;
import com.iota.iri.storage.ZmqPublishProvider;

/**
 * Created by paul on 5/19/17.
 */
public class IotaWrapper extends com.iota.iri.Iota {

    public IotaWrapper(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void init() throws Exception {
        this.initializeTangle();
        this.tangle.init();
        // rescan 被阉割了

        boolean revalidate = this.configuration.booling(Configuration.DefaultConfSettings.REVALIDATE);
        if (revalidate) {
            this.tangle.clearColumn(com.iota.iri.model.Milestone.class);
            this.tangle.clearColumn(StateDiff.class);
            this.tangle.clearMetadata(Transaction.class);
        }

        this.milestone.init(SpongeFactory.Mode.CURLP27, this.ledgerValidator, revalidate);
        this.transactionValidator.init(this.testnet, this.configuration.integer(Configuration.DefaultConfSettings.MWM));
        this.tipsManager.init();
        this.transactionRequester.init(this.configuration.doubling(Configuration.DefaultConfSettings.P_REMOVE_REQUEST.name()));
        this.udpReceiver.init();
        this.replicator.init();
        this.node.init();
    }

    private void initializeTangle() {
        String dbPath = configuration.string(Configuration.DefaultConfSettings.DB_PATH);
        if (testnet) {
            if (dbPath.isEmpty() || dbPath.equals("mainnetdb")) {
                // testnetusers must not use mainnetdb, overwrite it unless an explicit name is set.
                configuration.put(Configuration.DefaultConfSettings.DB_PATH.name(), "testnetdb");
                configuration.put(Configuration.DefaultConfSettings.DB_LOG_PATH.name(), "testnetdb.log");
            }
        } else {
            if (dbPath.isEmpty() || dbPath.equals("testnetdb")) {
                // mainnetusers must not use testnetdb, overwrite it unless an explicit name is set.
                configuration.put(Configuration.DefaultConfSettings.DB_PATH.name(), "mainnetdb");
                configuration.put(Configuration.DefaultConfSettings.DB_LOG_PATH.name(), "mainnetdb.log");
            }
        }
        // 忽略参数配置, 直接使用InnoDb
        tangle.addPersistenceProvider(new InnoDBPersistenceProvider("mybatis-config.xml"));
        if (configuration.booling(Configuration.DefaultConfSettings.EXPORT)) {
            tangle.addPersistenceProvider(new FileExportProvider());
        }
        if (configuration.booling(Configuration.DefaultConfSettings.ZMQ_ENABLED)) {
            tangle.addPersistenceProvider(new ZmqPublishProvider(messageQ));
        }
    }
}

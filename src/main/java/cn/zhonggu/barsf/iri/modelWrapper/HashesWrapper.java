package cn.zhonggu.barsf.iri.modelWrapper;

import com.iota.iri.model.Hashes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * Created by paul on 3/8/17 for iri.
 */

public class HashesWrapper extends Hashes {
    private static final Logger log = LoggerFactory.getLogger(Hashes.class);
    @Id
    @Column
    @GeneratedValue(strategy = GenerationType.IDENTITY, generator = "Mysql")
    private String hash;

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }
}

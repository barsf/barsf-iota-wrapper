package cn.zhonggu.barsf.iri.modelWrapper;

import com.iota.iri.model.Hash;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.model.Milestone;

import javax.persistence.*;

/**
 * Created by paul on 4/11/17.
 */
@Table(name = "t_milestone")
public class MilestoneWrapper {
    public MilestoneWrapper(Milestone milestone) {
        this.index = milestone.index.getValue();
        this.hash = milestone.hash.toString();
    }

    public MilestoneWrapper() {
    }

    @Id
    @Column(name = "mt_index")
    @GeneratedValue(strategy = GenerationType.IDENTITY, generator = "Mysql")
    public Integer index;

    @Column(name = "tx_hash")
    public String hash;

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public Milestone toMilestone() {
        Milestone milestone = new Milestone();
        milestone.hash = new Hash(this.hash);
        milestone.index = new IntegerIndex(this.index);
        return milestone;
    }
}

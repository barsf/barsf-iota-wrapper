package cn.zhonggu.barsf.iri.modelWrapper;

import com.iota.iri.model.Hash;
import com.iota.iri.model.Tag;

public class ObsoleteTag extends Tag {

    public ObsoleteTag(Hash hash) {
        super(hash);
    }

    // used by the persistence layer to instantiate the object
    public ObsoleteTag() {

    }
}

package cn.zhonggu.barsf.iri.modelWrapper;

import com.iota.iri.model.Hash;
import com.iota.iri.model.Tag;

import java.util.List;

/**
 * Created by paul on 5/15/17.
 */
public class TagWrapper extends HashesWrapper {
    public TagWrapper(Hash hash) {
        set.add(hash);
    }

    public TagWrapper(List<Hash> hashes){
        set.addAll(hashes);
    }

    public TagWrapper() {

    }

    public Tag toTag(){
        Tag tag = new Tag();
        tag.set.addAll(this.set);
        return tag;
    }
}

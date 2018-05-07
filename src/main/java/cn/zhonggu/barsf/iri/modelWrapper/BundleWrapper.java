package cn.zhonggu.barsf.iri.modelWrapper;

import com.iota.iri.model.Bundle;
import com.iota.iri.model.Hash;

import java.util.List;

/**
 * Created by paul on 5/15/17.
 */
public class BundleWrapper extends HashesWrapper {

    public BundleWrapper(Hash hash) {
        set.add(hash);
    }

    public BundleWrapper(List<Hash> hash) {
        set.addAll(hash);
    }
    public BundleWrapper() {

    }

    public Bundle toBundle(){
        Bundle bundle = new Bundle();
        bundle.set.addAll(this.set);
        return bundle;
    }
}

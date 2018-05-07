package cn.zhonggu.barsf.iri.modelWrapper;

import com.iota.iri.model.Approvee;
import com.iota.iri.model.Hash;

import java.util.List;

/**
 * Created by paul on 5/15/17.
 */
public class ApproveeWrapper extends HashesWrapper {
    public ApproveeWrapper(Hash hash) {
        set.add(hash);
    }

    public ApproveeWrapper(List<Hash> hashes){
        set.addAll(hashes);
    }

    public ApproveeWrapper() {

    }

    public Approvee toApprovee(){
        Approvee approvee = new Approvee();
        approvee.set.addAll(this.set);
        return approvee;
    }
}

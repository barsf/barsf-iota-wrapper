package cn.zhonggu.barsf.iri.modelWrapper;

import com.iota.iri.model.Address;
import com.iota.iri.model.Hash;

import javax.persistence.Table;
import java.util.Collection;


/**
 * Created by paul on 5/15/17.
 */
@Table(name = "t_address")
public class AddressWrapper extends HashesWrapper {
    public AddressWrapper() {
    }
//    private Long balance = 0L;

    private Long barsfBalance = 0L;

//    public Long getBalance() {
//        return balance;
//    }
//
//    public void setBalance(Long balance) {
//        this.balance = balance;
//    }

    public Long getBarsfBalance() {
        return barsfBalance;
    }

    public void setBarsfBalance(Long barsfBalance) {
        this.barsfBalance = barsfBalance;
    }



    public AddressWrapper(Hash hash) {
        set.add(hash);
    }
    public AddressWrapper(Collection<Hash> hashes) {
        set.addAll(hashes);
    }

    public Address toAddress(){
        Address address = new Address();
        address.set.addAll(this.set);
        return address;
    }
}

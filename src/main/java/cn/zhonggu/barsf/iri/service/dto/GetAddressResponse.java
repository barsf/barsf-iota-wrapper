package cn.zhonggu.barsf.iri.service.dto;

import cn.zhonggu.barsf.iri.modelWrapper.AddressWrapper;
import com.iota.iri.service.dto.AbstractResponse;

import java.util.HashMap;

/**
 * Created by ZhuDH on 2018/5/8.
 */
public class GetAddressResponse extends AbstractResponse {
    private GetAddressResponse(HashMap<String, AddressWrapper> balances) {
        this.balances = balances;
    }
    private HashMap<String,AddressWrapper> balances;

    public HashMap<String, AddressWrapper> getBalances() {
        return balances;
    }

    public void setBalances(HashMap<String, AddressWrapper> balances) {
        this.balances = balances;
    }

    public static GetAddressResponse create(HashMap<String, AddressWrapper> balances) {
        return new GetAddressResponse(balances);
    }
}

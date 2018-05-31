package cn.zhonggu.barsf.iri.service.dto;

import com.iota.iri.service.dto.AbstractResponse;

import java.util.List;

/**
 * Created by bfq on 2018/5/31.
 */
public class GetBarsfBalancesResponse extends AbstractResponse {
    private List<Long> balances;

    //获取地址余额时，对应的barsf里程碑的hash值
    private List<String> reference;

    //里程碑对应的序号
    private int milestoneIndex;

    public GetBarsfBalancesResponse(List<Long> balances, List<String> reference, int milestoneIndex) {
        this.balances = balances;
        this.reference = reference;
        this.milestoneIndex = milestoneIndex;
    }

    public List<Long> getBalances() {
        return balances;
    }

    public void setBalances(List<Long> balances) {
        this.balances = balances;
    }

    public List<String> getReference() {
        return reference;
    }

    public void setReference(List<String> reference) {
        this.reference = reference;
    }

    public int getMilestoneIndex() {
        return milestoneIndex;
    }

    public void setMilestoneIndex(int milestoneIndex) {
        this.milestoneIndex = milestoneIndex;
    }

    public static GetBarsfBalancesResponse create(List<Long> balances, List<String> reference, int milestoneIndex) {
        return new GetBarsfBalancesResponse(balances, reference, milestoneIndex);
    }
}

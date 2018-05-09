package cn.zhonggu.barsf.iri.service.dto;

import com.iota.iri.service.dto.AbstractResponse;

import java.util.List;

public class TestResponse extends AbstractResponse {

    private TestResponse(List<String> guys){
        for(String gay: guys) {
            hello += gay+" ";
        }
    }
    private  String hello = "hello";

    public  String getHello() {
        return hello;
    }

    public  void setHello(String hello) {
        this.hello = hello;
    }

    public static TestResponse create(final List<String> guys) {
        return new TestResponse(guys);
    }

}

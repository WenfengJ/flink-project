package com.longyun.flink.cep;

/**
 * @author lynn
 * @ClassName com.longyun.flink.cep.LoginEvent
 * @Description TODO
 * @Date 19-3-12 下午5:56
 * @Version 1.0
 **/
public class LoginEvent {

    private String userId;

    private String ip;

    private String type;

    public LoginEvent(){

    }

    public LoginEvent(String userId, String ip, String type) {
        this.userId = userId;
        this.ip = ip;
        this.type = type;
    }

    public String getUserId() {
        return userId;
    }

    public String getIp() {
        return ip;
    }

    public String getType() {
        return type;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setType(String type) {
        this.type = type;
    }

    public LoginEvent withUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public LoginEvent withIp(String ip) {
        this.ip = ip;
        return this;
    }

    public LoginEvent withType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ip='" + ip + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}

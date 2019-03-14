package com.longyun.flink.cep;

/**
 * @author lynn
 * @ClassName com.longyun.flink.cep.LoginWarning
 * @Description TODO
 * @Date 19-3-12 下午6:19
 * @Version 1.0
 **/
public class LoginWarning {

    private String userId;
    
    private String type;
    
    private String ip;

    public LoginWarning(){

    }

    public LoginWarning(String userId, String ip, String type) {
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

    public LoginWarning withUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public LoginWarning withIp(String ip) {
        this.ip = ip;
        return this;
    }

    public LoginWarning withType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String toString() {
        return "LoginWarning{" +
                "userId='" + userId + '\'' +
                ", type='" + type + '\'' +
                ", ip='" + ip + '\'' +
                '}';
    }
}

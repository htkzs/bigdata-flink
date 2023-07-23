package com.mashibing.flinkjava.code.chapter6;

/**
 * StationLog基站日志类
 *  sid:基站ID
 *  callOut: 主叫号码
 *  callIn: 被叫号码
 *  callType: 通话类型，失败（fail）/占线（busy）/拒接（barring）/接通（success）
 *  callTime: 呼叫时间戳，毫秒
 *  duration: 通话时长，秒
 */
public class StationLog {
    public String sid;
    public String callOut;
    public String callIn;
    public String callType;
    public Long callTime;
    public Long Duration;

    public StationLog() {
    }

    public StationLog(String sid, String callOut, String callIn, String callType, Long callTime, Long duration) {
        this.sid = sid;
        this.callOut = callOut;
        this.callIn = callIn;
        this.callType = callType;
        this.callTime = callTime;
        Duration = duration;
    }

    @Override
    public String toString() {
        return "StationLog{" +
                "sid='" + sid + '\'' +
                ", callOut='" + callOut + '\'' +
                ", callIn='" + callIn + '\'' +
                ", callType='" + callType + '\'' +
                ", callTime=" + callTime +
                ", Duration=" + Duration +
                '}';
    }
}

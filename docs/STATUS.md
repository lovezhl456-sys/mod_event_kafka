# 验证状态

更新日期：2026-09-30。本页区分历史报告与本次检查。**未经生产验证，也没有穷尽所有故障。**

## 本次改动

- 增加真正的三 broker 栈、同一发送进程的恢复演练、八组记录校验对照。
- 验收器要求非空注入、完整终态和显式过期策略；顺序/自愈未检查时明确输出 NOT_CHECKED。
- 故障注入失败或收到信号时恢复本次代理改动；ASan/UBSan 覆盖 core。
- [本次验证记录](VALIDATION-20260930.md)：core/ASan/UBSan、21 项工具测试、六轮恢复与八组记录对照通过；严格消费顺序实测失败，未修改生产排序实现。

## 历史报告（保留定位信息，原始 reports 不在 Git）

| 范围 | 报告结果 | 证据目录（相对原实验室工作区） |
|---|---|---|
| core 冒烟 | 2026-09-25 enqueued=produce_ok=acked=20；修复 poll 持锁后通过 | 原稳定测试架记录 |
| L-02/L-07，稳定测试架 | 35s 全入口断连，reload 前 60/60，producer_rebuilds=0 | `reports/l02-l07-run1790313218/` |
| L-02/L-07，FS Phase2 | 报告 FS 1.10.12，60/60，无 reload；park 不保证产生 ANSWER | `reports/l02-l07-fs-20260925-133618/` |
| L-16a/b/c，FS + TTL | 35s：60/60；150s：30 消费、30 expired_ttl；恢复后新事件 30/30 | `reports/l16-abc-20260925-215439/` |
| FS-11 主证据，旧模块 + FS 1.6.20 | compact/null：670 行 +87；delete/null：0 次 | `reports/fs11-err87-fs16-20260930-162827/` |
| FS-11 较早对照，旧模块 + FS 1.10.x | compact/null：482 行；delete/null：0 次；另有 compact/key 成功 | `reports/fs11-err87-20260930-154534/` |

这些 PASS 是原报告主张；不能在缺少原始消费/终态证据时用新验收器追认。候选补丁 `0001` 仍为 NOT_VERIFIED。

## FS-11 主证据定位

- 原实验室工作区：`/workspace/mod_event_kafka-fix/`；工具链：`/workspace/lab-mod-event-kafka/fs16/`。
- 主证据含 RESULT、`fs-version.txt`、`topic-config.txt`、`reproduce.sh`、`REBUILD-SO.md`、broker-reason 和模块说明。
- FS 二进制：`1.6.20+git~20180123T214909Z~987c9b9a2a~64bit`；镜像 `lab-freeswitch:1.6.20-kafka`，id `f057ade5fc2f`。
- 旧 `.so` MD5：`063e55cb0a584839f68d44a22dab62e6`；安装目录 `/usr/local/freeswitch/lib/freeswitch/mod/`；失败回调在旧模块第 197 行。
- 该轮 librdkafka 为实验室 1.9.2，不能推定与生产旧库相同。
- 早期 Jessie 运行时镜像缺头文件而受阻，后来才用源码构建的 FS 1.6.20 重跑。早期 FS 1.10.x 二进制实际报告 `1.10.7-dev+git~20210825T173719Z~dd2411336f~64bit`，不能把镜像标签当作运行时版本。

## 不能据此宣称

所有 L-03–L-15 完整通过、云厂商发布复现、旧生产库协议回退、严格消费顺序、fatal 重建安全。下一步见 [覆盖矩阵](FAULT-SCENARIOS.md)。

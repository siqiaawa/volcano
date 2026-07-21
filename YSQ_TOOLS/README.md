# Kind 宿主机检测工具

`kind-host-diagnose.sh` 用于检查 Linux 服务器能否启动 Volcano HyperNode
E2E 使用的 Kind 节点镜像，重点诊断以下错误：

```text
could not find a log line that matches
"Reached target .*Multi-User System.*|detected cgroup v1"
```

## 安全边界

脚本不会修改 Docker 配置、不会重启 Docker、不会安装工具，也不会修改
Volcano 源代码。它会创建一个临时的单 control-plane Kind 集群：

- 创建成功：采集信息后默认自动删除。
- 创建失败：保留失败节点，以便导出 Docker、systemd 和 Kind 日志。
- 最终报告默认写入 `$HOME/volcano-kind-diagnostics` 并压缩为 `.tar.gz`。

## 在服务器上运行

进入此前克隆的仓库并拉取功能分支：

```bash
cd /root/work/volcano-mixed-topology
git pull --ff-only origin feature/mixed-topology-hypernode
```

确认当前提交，然后给脚本增加执行权限：

```bash
git rev-parse HEAD
chmod +x YSQ_TOOLS/kind-host-diagnose.sh
```

运行完整检测：

```bash
./YSQ_TOOLS/kind-host-diagnose.sh
```

脚本会使用和项目 E2E 完全相同的 Kubernetes v1.36.1 Kind node image。
执行可能持续几分钟。结束时会打印报告目录和压缩包路径，例如：

```text
Archive: /root/volcano-kind-diagnostics/kind-host-diagnostics-20260721-180000.tar.gz
```

## 失败集群的处理

如果 smoke test 失败，脚本会保留调试集群，并在输出中打印清理命令：

```text
Cleanup command after investigation:
kind delete cluster --name volcano-kind-debug-20260721-180000
```

先取回报告压缩包，再执行脚本打印的准确命令清理。不要提前删除，否则可能
丢失失败节点现场。

## 从 Windows 取回报告

退出 SSH 或另开一个 Windows PowerShell，使用脚本实际打印的压缩包路径：

```powershell
scp root@192.168.2.30:/root/volcano-kind-diagnostics/kind-host-diagnostics-20260721-180000.tar.gz G:\~CODE\VOLCANO\
```

把服务器地址和时间戳替换为实际值。下载后把 `.tar.gz` 文件路径发给 Codex
即可继续分析。

## 可选参数

只采集宿主信息，不创建 Kind 集群：

```bash
./YSQ_TOOLS/kind-host-diagnose.sh --skip-kind
```

即使创建成功也保留调试集群：

```bash
./YSQ_TOOLS/kind-host-diagnose.sh --keep-cluster
```

把报告保存到指定目录：

```bash
./YSQ_TOOLS/kind-host-diagnose.sh \
  --output-dir /root/work/volcano-test-logs
```

查看全部参数：

```bash
./YSQ_TOOLS/kind-host-diagnose.sh --help
```

诊断报告可能包含服务器 hostname、容器内部地址、镜像名称和 registry 配置，
对外分享前应先检查内容。

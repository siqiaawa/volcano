# Kind 宿主机检测工具

`kind-host-diagnose.sh` 用于检查 Linux 服务器能否启动 Volcano HyperNode
E2E 使用的 Kind 节点镜像，重点诊断以下错误：

```text
could not find a log line that matches
"Reached target .*Multi-User System.*|detected cgroup v1"
```

## 安全边界

脚本不会上传任何数据、不会修改 Docker 配置、不会重启 Docker、不会安装工具，也不会修改
Volcano 源代码。它会创建一个临时的单 control-plane Kind 集群：

- 创建成功：采集信息后默认自动删除。
- 创建失败：保留失败节点，以便导出 Docker、systemd 和 Kind 日志。
- 最终报告默认写入 `$HOME/volcano-kind-diagnostics`。

涉密服务器应使用 `--no-archive`，使结果只保留为服务器上的本地目录；不要执行
`scp`、不要上传压缩包，也不要把原始日志粘贴到外部系统。

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

在涉密服务器上运行完整检测：

```bash
./YSQ_TOOLS/kind-host-diagnose.sh --no-archive
```

脚本会使用和项目 E2E 完全相同的 Kubernetes v1.36.1 Kind node image。
执行可能持续几分钟。结束时会打印只存在于服务器本地的报告目录，例如：

```text
Report directory: /root/volcano-kind-diagnostics/kind-host-diagnostics-20260721-180000
```

## 失败集群的处理

如果 smoke test 失败，脚本会保留调试集群，并在输出中打印清理命令：

```text
Cleanup command after investigation:
kind delete cluster --name volcano-kind-debug-20260721-180000
```

先在服务器内部检查报告，再执行脚本打印的准确命令清理。不要提前删除，否则
可能丢失失败节点现场。

## 在服务器本地查看结论

找到最新报告目录：

```bash
ls -dt "$HOME"/volcano-kind-diagnostics/kind-host-diagnostics-* | head -n 1
```

把输出的实际目录代入以下命令：

```bash
cat /root/volcano-kind-diagnostics/kind-host-diagnostics-实际时间戳/diagnosis.txt
```

只查看不包含 hostname、IP 和原始日志的分类码：

```bash
grep '^RESULT_CODE=' \
  /root/volcano-kind-diagnostics/kind-host-diagnostics-实际时间戳/diagnosis.txt
```

分类码含义：

| 分类码 | 含义 |
|---|---|
| `KIND_OK` | 单节点 Kind 可以启动，五节点失败更可能是资源问题 |
| `DOCKER_UNREACHABLE` | Docker 不可访问 |
| `PREREQUISITE_MISSING` | kind 或 kubectl 缺失 |
| `DOCKER_LOGGING_DISABLED` | Docker 日志驱动为 `none` |
| `NODE_OOM` | Kind 节点被 OOM 杀死 |
| `CGROUP_PERMISSION` | cgroup、mount 或 privileged 权限不足 |
| `NESTED_CONTAINER` | 服务器本身是受限容器环境 |
| `UNKNOWN_LOCAL_REVIEW` | 需要在服务器内部继续查看本地日志 |

在单位安全制度允许的前提下，只需记录或口头描述 `RESULT_CODE`，不需要传输报告。

## 可选参数

只采集宿主信息，不创建 Kind 集群：

```bash
./YSQ_TOOLS/kind-host-diagnose.sh --skip-kind
```

不生成压缩包，只保留服务器本地目录：

```bash
./YSQ_TOOLS/kind-host-diagnose.sh --no-archive
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

完整诊断目录可能包含服务器 hostname、容器内部地址、镜像名称和 registry 配置。
涉密环境中应始终留在服务器内部，仅由有权限的人员本地查看。

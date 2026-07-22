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

如果复制上述命令时出现 `invalid refspec`，改为逐条执行，避免隐藏字符进入分支名：

```bash
git fetch origin
git switch feature/mixed-topology-hypernode
git merge --ff-only origin/feature/mixed-topology-hypernode
```

确认当前提交，然后给脚本增加执行权限：

```bash
git rev-parse HEAD
chmod +x YSQ_TOOLS/kind-host-diagnose.sh
```

在涉密服务器上给两个脚本增加执行权限：

```bash
chmod +x YSQ_TOOLS/kind-host-diagnose.sh
chmod +x YSQ_TOOLS/kind-quick-diagnose.sh
chmod +x YSQ_TOOLS/kind-node-quick-inspect.sh
```

运行简洁检测入口：

```bash
./YSQ_TOOLS/kind-quick-diagnose.sh
```

该入口自动启用 `--confidential`，不会生成压缩包，运行期间只显示开始提示；
详细日志保留在服务器本地。脚本使用和项目 E2E 完全相同的 Kubernetes v1.36.1
Kind node image，执行可能持续两分钟左右。

结束时只显示以下固定格式，不显示 hostname、IP 或原始日志：

```text
=== SAFE_KIND_RESULT ===
RESULT_CODE=READY_MARKER_MISSING
KIND_EXIT=1
CONTAINER_STATE=running
CONTAINER_EXIT=0
OOM_KILLED=false
NODE_LOG_DRIVER=json-file
PID1=systemd
SYSTEMD_STATE=running
MULTI_USER=active
FAILED_UNITS=0
DOCKER_LOG_LINES=10
DOCKER_READY_LINES=0
JOURNAL_READY_LINES=0
HOST_CGROUP_VERSION=2
CONTAINER_CGROUP_FS=cgroup2fs
NEXT_ACTION=KIND_SYSTEMD_READINESS_INCOMPATIBLE
CLEANUP_REQUIRED=yes
CLEANUP_CLUSTER=volcano-kind-debug-时间戳
=== END_SAFE_KIND_RESULT ===
```

只需要查看这个短结果区块。详细报告仍只存在于服务器本地，例如：

```text
Report directory: /root/volcano-kind-diagnostics/kind-host-diagnostics-20260721-180000
```

## 检查已经保留的失败节点

如果简洁检测已经返回 `NODE_CONTAINER_EXITED`，不要立即重新创建集群。使用结果中的
`CLEANUP_CLUSTER` 值检查现有容器。例如：

```bash
chmod +x YSQ_TOOLS/kind-node-quick-inspect.sh
./YSQ_TOOLS/kind-node-quick-inspect.sh \
  volcano-kind-debug-20260722-114209
```

该命令不会创建或删除集群，也不会显示原始日志。它只输出固定签名：

```text
=== SAFE_NODE_EXIT_RESULT ===
RESULT_CODE=CGROUP_INITIALIZATION_FAILED
ERROR_SIGNATURE=CGROUP_INIT_SCOPE_FAILED
CONTAINER_STATE=exited
CONTAINER_EXIT=255
OOM_KILLED=false
NODE_LOG_DRIVER=json-file
DOCKER_LOG_LINES=50
HOST_CGROUP_VERSION=2
NEXT_ACTION=CHECK_DOCKER_CGROUP_DELEGATION
CLEANUP_CLUSTER=volcano-kind-debug-20260722-114209
=== END_SAFE_NODE_EXIT_RESULT ===
```

常见安全签名：

| `ERROR_SIGNATURE` | 含义 |
|---|---|
| `CGROUP_INIT_SCOPE_FAILED` | systemd 无法创建 `/init.scope` cgroup |
| `CGROUP_READ_ONLY` | 容器内 cgroup 文件系统只读 |
| `SYSTEMD_MANAGER_FAILED` | systemd manager 初始化失败 |
| `CGROUP_INITIALIZATION_FAILED` | 其他 cgroup 初始化失败 |
| `NODE_MOUNT_PERMISSION` | 必要 mount 被宿主机拒绝 |
| `NODE_NO_SPACE` | Docker 磁盘或 inode 不足 |
| `NODE_MEMORY_FAILURE` | 节点启动期间内存分配失败 |
| `NODE_ARCH_MISMATCH` | 节点镜像架构不匹配 |
| `SYSTEMD_FATAL` | systemd 发生其他致命启动错误 |
| `OPERATION_NOT_PERMITTED` | 宿主机拒绝 privileged/cgroup 操作 |
| `UNCLASSIFIED_LOG` | 已有日志，但不匹配已知安全签名 |

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
| `NODE_CONTAINER_EXITED` | Kind 节点容器提前退出 |
| `CGROUP_INITIALIZATION_FAILED` | systemd 无法初始化 cgroup 层级 |
| `SYSTEMD_FATAL` | systemd 启动时发生致命错误 |
| `NODE_MOUNT_PERMISSION` | 宿主机拒绝节点所需 mount |
| `NODE_NO_SPACE` | Docker 磁盘或 inode 不足 |
| `NODE_MEMORY_FAILURE` | 节点启动期间内存分配失败 |
| `NODE_ARCH_MISMATCH` | 节点镜像与宿主机架构不匹配 |
| `CGROUP_PERMISSION` | cgroup、mount 或 privileged 权限不足 |
| `NESTED_CONTAINER` | 服务器本身是受限容器环境 |
| `SYSTEMD_NOT_PID1` | Kind 节点 PID 1 不是 systemd |
| `SYSTEMD_NOT_READY` | systemd 一直停留在 starting/initializing |
| `SYSTEMD_FAILED` | systemd 进入 maintenance/emergency/failed |
| `MULTI_USER_INACTIVE` | multi-user target 未激活 |
| `SYSTEMD_DEGRADED` | systemd 已启动但存在失败 unit |
| `DOCKER_LOG_EMPTY` | 运行中的节点没有 Docker stdout 日志 |
| `READY_LOG_NOT_EXPORTED` | journal 有就绪标记，但 Docker stdout 没有 |
| `READY_MARKER_MISSING` | multi-user 已激活，但两类日志都没有 Kind 等待的标记 |
| `KIND_LOG_WAIT_MISMATCH` | Docker 日志已有标记，但 Kind 没有消费到 |
| `SYSTEMD_QUERY_FAILED` | 节点运行中，但无法通过 Docker exec 查询 systemd |
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

隐藏过程输出，只显示安全结果区块：

```bash
./YSQ_TOOLS/kind-host-diagnose.sh --concise
```

同时启用不打包和简洁输出：

```bash
./YSQ_TOOLS/kind-host-diagnose.sh --confidential
```

推荐直接使用等价的快捷入口：

```bash
./YSQ_TOOLS/kind-quick-diagnose.sh
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

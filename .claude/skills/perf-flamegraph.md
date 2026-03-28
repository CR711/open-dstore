# perf-flamegraph

在 dstore Docker 容器（dstore_env）中对指定进程名抓取 CPU 火焰图。

容器内项目路径为 `/opt/project/dstore/`，对应宿主机的当前项目目录（通过 `-v` 挂载）。

## 用法

```
/perf-flamegraph <进程名> [采样时长(秒)]
```

**参数：**
- `<进程名>`：容器内的进程名，例如 `sysbenchtest`、`tpcctest`（必填）
- `[采样时长]`：perf record 持续时间，单位秒，默认 30（可选）

**示例：**
```
/perf-flamegraph sysbenchtest
/perf-flamegraph sysbenchtest 60
/perf-flamegraph tpcctest 45
```

## 执行步骤

收到此指令后，**直接执行以下所有步骤，无需询问确认**：

### 第 0 步：检查容器是否以 privileged 模式运行

```bash
docker inspect dstore_env --format '{{.HostConfig.Privileged}}'
```

- 若输出 `true`：继续下一步。
- 若输出 `false` 或容器不存在：**停止执行**，提示用户按以下方式重建容器：

  ```bash
  # 停止并删除旧容器
  docker stop dstore_env && docker rm dstore_env

  # 以 privileged 模式重新启动（路径根据实际情况调整）
  docker run -d \
    --name dstore_env \
    --privileged \
    -v <宿主机项目路径>:/opt/project/dstore \
    -v <宿主机依赖库路径>:/opt/project/local_libs \
    dstore:ubuntu
  ```

  说明：`--privileged` 是 perf 写入 `/proc/sys/kernel/perf_event_paranoid` 的必要条件，缺少时 perf record 无法采集硬件 PMU 事件。

### 第 1 步：检查进程是否存在

```bash
docker exec dstore_env pgrep -x <进程名>
```

若无输出（进程不存在），输出提示："进程 `<进程名>` 未在容器中运行，请先启动该进程后再抓取火焰图。" 然后停止。

### 第 2 步：设置 perf 权限

```bash
docker exec dstore_env bash -c "echo -1 > /proc/sys/kernel/perf_event_paranoid && echo 0 > /proc/sys/kernel/kptr_restrict"
```

若写入失败（Permission denied），说明容器未以 `--privileged` 启动，参考第 0 步提示重建容器。

### 第 3 步：对进程 attach 采样

```bash
docker exec dstore_env bash -c "
  PIDS=\$(pgrep -x <进程名> | tr '\n' ',' | sed 's/,\$//')
  /usr/lib/linux-tools/5.4.0-216-generic/perf record \
    -g --call-graph dwarf -F 99 \
    --pid \$PIDS \
    -o /opt/project/dstore/perf.data \
    -- sleep <采样时长>
"
```

输出采样进度，等待完成。

### 第 4 步：生成火焰图 SVG

```bash
docker exec dstore_env bash -c "
  /usr/lib/linux-tools/5.4.0-216-generic/perf script -i /opt/project/dstore/perf.data 2>/dev/null \
    | /opt/FlameGraph/stackcollapse-perf.pl \
    | /opt/FlameGraph/flamegraph.pl --title='<进程名> CPU Flame Graph' --width=1600 \
    > /tmp/fg_<进程名>.svg
  cp /tmp/fg_<进程名>.svg /opt/project/dstore/flamegraph_<进程名>.svg
"
```

### 第 5 步：确认结果

检查 SVG 是否存在且大小合理（正常应在 100KB 以上）：

```bash
# 容器内确认
docker exec dstore_env ls -lh /opt/project/dstore/flamegraph_<进程名>.svg

# 宿主机确认（通过挂载目录同步）
ls -lh "$(pwd)/flamegraph_<进程名>.svg"
```

输出 SVG 的宿主机完整路径，提示用户用浏览器打开查看。

## 注意事项

- **容器必须以 `--privileged` 模式启动**，否则无法写入 perf 内核参数（第 0 步会提前检查）
- perf 二进制路径：`/usr/lib/linux-tools/5.4.0-216-generic/perf`（容器内已安装）
- FlameGraph 脚本路径：`/opt/FlameGraph/`（容器内已安装）
- 采样期间**目标进程必须持续运行**，否则 perf attach 会立即退出
- 生成的 `flamegraph_<进程名>.svg` 和 `perf.data` 均输出到容器内 `/opt/project/dstore/`，通过挂载自动同步到宿主机项目根目录

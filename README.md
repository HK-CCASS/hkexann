# HKEX 公告智能下载与向量化系统

港交所（HKEX）公告自动下载、智能分类、PDF 解析与向量化存储系统。支持历史批量回填与实时监听，向量数据存入 Milvus 供 RAG 检索使用。

---

## 目录

- [系统架构](#系统架构)
- [前置要求](#前置要求)
- [部署（Mac Mini / 新机器）](#部署mac-mini--新机器)
- [配置说明](#配置说明)
- [日常使用](#日常使用)
- [自动化定时任务](#自动化定时任务)
- [数据迁移](#数据迁移)

---

## 系统架构

```
HKEX API
    │
    ▼
双重过滤（股票白名单 + 公告类型）
    │
    ▼
异步下载 PDF → 智能分类落盘（/Volumes/SSD/DockerData/hkexann_data/）
    │
    ▼
PDF 解析 → 分块（chunk）→ SiliconFlow Embedding API
    │
    ▼
Milvus 向量库（Docker，数据存于外接 SSD）
```

**关键路径：**

| 数据 | 路径 |
|------|------|
| 下载的 PDF | `/Volumes/SSD/DockerData/hkexann_data/` |
| Milvus 向量数据 | `/Volumes/SSD/DockerData/milvus_data/` |
| Docker 磁盘镜像 | `/Volumes/SSD/DockerData/DockerDesktop` |

---

## 前置要求

- macOS（Apple Silicon，M1/M2/M3/M4）
- Python 3.9+
- Docker Desktop（磁盘镜像指向外接 SSD，见下文）
- 外接 SSD 挂载于 `/Volumes/SSD/`
- [SiliconFlow](https://siliconflow.cn) API Key（用于 Embedding）

---

## 部署（Mac Mini / 新机器）

> 本节为完整部署流程，由 Claude Code 协助执行。

### 第一步：确认外接 SSD 已挂载

```bash
ls /Volumes/SSD/DockerData/
# 应显示：DockerDesktop  hkexann_data  milvus_data
```

如路径不同（如挂载为 `/Volumes/SSD2/`），记录实际路径，后续配置中替换。

### 第二步：安装 Docker Desktop

1. 下载 [Docker Desktop for Mac（Apple Silicon）](https://www.docker.com/products/docker-desktop/)
2. 安装后，**打开设置 → Resources → Advanced**
3. 将「Disk image location」改为：`/Volumes/SSD/DockerData/DockerDesktop`
4. 点击 Apply & Restart

> 如 SSD 上已有旧机器的 Docker 磁盘镜像，Docker 可直接复用，Milvus 镜像无需重新 pull。

### 第三步：克隆项目

```bash
git clone https://github.com/<your-username>/hkexann.git
cd hkexann
```

### 第四步：创建虚拟环境并安装依赖

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 第五步：配置环境变量

```bash
cp .env.template .env
```

编辑 `.env`，填入以下必填项：

```env
# SiliconFlow Embedding API（必填）
SILICONFLOW_API_KEY=sk-xxxxxxxxxxxxxxxx

# Milvus（默认端口 19531，通常无需修改）
MILVUS_HOST=localhost
MILVUS_PORT=19531

# PDF 存储路径（与 SSD 挂载路径一致）
PDF_DATA_PATH=/Volumes/SSD/DockerData/hkexann_data/HKEX

# 数据库（可选，如不使用 MySQL/ClickHouse 可留空）
# DB_PASSWORD=
# CLICKHOUSE_PASSWORD=
# CCASS_PASSWORD=
```

### 第六步：确认 config.yaml 路径正确

检查 `config.yaml` 中的路径与当前 SSD 挂载路径一致：

```bash
grep -E "save_path|download_directory" config.yaml
```

如 SSD 路径发生变化（如从 `/Volumes/SSD/` 变为其他），修改对应字段：

```yaml
settings:
  save_path: "/Volumes/SSD/DockerData/hkexann_data"

downloader_integration:
  download_directory: "/Volumes/SSD/DockerData/hkexann_data"
```

### 第七步：启动 Milvus

```bash
docker compose -f scripts/docker-compose.milvus.yml up -d
```

等待约 30 秒后验证：

```bash
docker compose -f scripts/docker-compose.milvus.yml ps
# 三个容器（milvus-standalone, milvus-etcd, milvus-minio）均应为 healthy
```

验证向量库数据（从旧机器迁移时确认数据完整）：

```bash
python3 - <<'EOF'
from pymilvus import connections, Collection
connections.connect(host="localhost", port=19531)
col = Collection("pdf_embeddings_v3")
col.load()
print(f"向量库记录数: {col.num_entities}")
EOF
```

### 第八步：运行冒烟测试

```bash
# 单股票、短时间范围的验证测试
python3 manual_historical_backfill.py -s 00209 --date-range 2026-01-01 2026-03-01 --dry-run
```

---

## 配置说明

### 主配置文件 `config.yaml`

| 字段 | 说明 |
|------|------|
| `settings.save_path` | PDF 下载根目录 |
| `downloader_integration.download_directory` | 同上，供监听器使用 |
| `milvus.host` / `milvus.port` | Milvus 连接地址 |
| `common_keywords` | 公告分类关键字 |
| `announcement_categories` | 公告类型白名单 |

### 环境变量 `.env`

| 变量 | 必填 | 说明 |
|------|------|------|
| `SILICONFLOW_API_KEY` | ✅ | SiliconFlow API 密钥 |
| `MILVUS_HOST` | ✅ | Milvus 主机（默认 localhost） |
| `MILVUS_PORT` | ✅ | Milvus 端口（默认 19531） |
| `PDF_DATA_PATH` | ✅ | PDF 存储路径 |
| `DB_PASSWORD` | ⬜ | MySQL 密码（可选） |
| `CLICKHOUSE_PASSWORD` | ⬜ | ClickHouse 密码（可选） |

---

## 日常使用

### 历史批量回填

```bash
source venv/bin/activate

# 单股票
python3 manual_historical_backfill.py -s 00700 --date-range 2025-01-01 2026-03-01

# 从股票列表文件
python3 manual_historical_backfill.py \
  -s '/path/to/stock_list.txt' \
  --date-range 2025-01-01 2026-03-01

# 试运行（不实际下载）
python3 manual_historical_backfill.py -s 00700 -d 7 --dry-run
```

**参数说明：**

| 参数 | 说明 |
|------|------|
| `-s` | 股票代码、逗号分隔列表、或 `.txt` 文件路径 |
| `--date-range <start> <end>` | 指定日期范围（YYYY-MM-DD） |
| `-d <days>` | 最近 N 天（与 `--date-range` 二选一） |
| `--dry-run` | 试运行，仅解析不下载 |
| `--no-filter` | 禁用公告类型过滤，下载所有类型 |
| `-b <n>` | 批次大小（默认 5） |

**断点续传：** 中途按 `Ctrl+C` 可优雅退出，进度保存在 `hkexann/.announcement_checkpoint.json`，下次运行自动续传。

### 实时监听

```bash
# 启动实时监听（每 60 秒轮询一次）
python3 start_enhanced_monitor.py

# 测试模式
python3 start_enhanced_monitor.py -t
```

### Milvus 维护

```bash
# 执行碎片合并（首次大批量写入后建议执行）
python3 - <<'EOF'
from pymilvus import connections, Collection
connections.connect(host="localhost", port=19531)
col = Collection("pdf_embeddings_v3")
print("开始 compaction...")
col.compact()
col.wait_for_compaction_completed()
print("完成")
EOF
```

---

## 自动化定时任务（macOS launchd）

每日港股时间 02:00–07:00 自动扫描并下载新公告：

```bash
# 复制 plist 到 LaunchAgents
cp scripts/com.hkexann.nightly.plist ~/Library/LaunchAgents/

# 注册定时任务
launchctl load ~/Library/LaunchAgents/com.hkexann.nightly.plist

# 验证
launchctl list | grep hkexann
```

**前提：**

1. 编辑 `scripts/nightly_download.sh`，将 `STOCK_LIST_FILE` 改为本机实际股票列表路径。
2. 编辑 `scripts/com.hkexann.nightly.plist`，将所有 `/path/to/your/hkexann` 替换为本机实际项目路径（如 `/Users/username/hkexann`）。

---

## 数据迁移

将外接 SSD 插入新 Mac 即可继承所有数据，无需额外操作：

```
SSD 上的数据（随盘迁移）：
├── /Volumes/SSD/DockerData/hkexann_data/   ← 全部 PDF 文件
├── /Volumes/SSD/DockerData/milvus_data/    ← 全部向量数据
└── /Volumes/SSD/DockerData/DockerDesktop   ← Docker 磁盘镜像
```

迁移后在新机器上只需执行第一步至第八步完成部署，向量库数据自动恢复。

---

## 项目结构

```
hkexann/
├── manual_historical_backfill.py   # 历史批量回填入口
├── start_enhanced_monitor.py       # 实时监听入口
├── config.yaml                     # 主配置文件
├── .env.template                   # 环境变量模板
├── requirements.txt
├── scripts/
│   ├── docker-compose.milvus.yml   # Milvus Docker 配置
│   ├── nightly_download.sh         # 定时下载脚本
│   └── com.hkexann.nightly.plist   # launchd 任务配置
├── services/
│   ├── monitor/                    # 监听与过滤
│   ├── document_processor/         # PDF 解析与向量化
│   ├── embeddings/                 # SiliconFlow Embedding 客户端
│   └── milvus/                     # Milvus 集合管理
└── docs/
    └── 执行报告/                   # 变更记录
```

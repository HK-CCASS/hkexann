#!/bin/bash
# ============================================================
# HKEX 夜间公告自动下载脚本
# 运行时段: HKT 02:00 - 07:00（每30分钟扫描一次）
# 由 launchd 在 HKT 02:00 启动，到 07:00 自动退出
# ============================================================

set -euo pipefail

# 项目目录（脚本所在目录的上一级）
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="python3"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/nightly_$(date '+%Y-%m-%d').log"
SSD_MOUNT="/Volumes/SSD"
STOCK_LIST_FILE="/Users/jet/HONG_KONG_STOCKS/stock_lists/供股.txt"

# 确保日志目录存在
mkdir -p "$LOG_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S HKT')] $*" | tee -a "$LOG_FILE"
}

# ── 前置检查 ─────────────────────────────────────────────────

check_ssd() {
    if [ ! -d "$SSD_MOUNT/DockerData/hkexann_data" ]; then
        log "❌ SSD 未挂载或路径不存在: $SSD_MOUNT"
        return 1
    fi
    return 0
}

check_milvus() {
    if ! docker compose -f "$PROJECT_DIR/scripts/docker-compose.milvus.yml" ps --status running 2>/dev/null | grep -q "milvus-standalone"; then
        log "⚠️  Milvus 未运行，尝试启动..."
        docker compose -f "$PROJECT_DIR/scripts/docker-compose.milvus.yml" up -d 2>>"$LOG_FILE"
        sleep 30   # 等待 Milvus 就绪
    fi
}

# ── 时间检查：HKT 07:00 后退出 ───────────────────────────────

is_within_window() {
    # 获取当前 HKT 小时（macOS date 使用 TZ 环境变量）
    local hour
    hour=$(TZ="Asia/Hong_Kong" date '+%H')
    # 2 <= hour < 7 → 继续运行
    if [ "$hour" -ge 7 ]; then
        return 1   # 超出窗口，退出
    fi
    return 0
}

# ── 主循环 ───────────────────────────────────────────────────

log "======================================================"
log "🌙 HKEX 夜间下载任务启动"
log "📂 项目目录: $PROJECT_DIR"
log "💾 SSD 路径: $SSD_MOUNT/DockerData/hkexann_data"
log "======================================================"

# 检查 SSD
if ! check_ssd; then
    log "❌ SSD 未就绪，任务中止"
    exit 1
fi

# 确保 Milvus 运行
check_milvus

cd "$PROJECT_DIR"

ROUND=0
while is_within_window; do
    ROUND=$((ROUND + 1))

    # 计算日期范围：今天和昨天（确保不遗漏跨午夜的公告）
    START_DATE=$(TZ="Asia/Hong_Kong" date -v-1d '+%Y-%m-%d')
    END_DATE=$(TZ="Asia/Hong_Kong" date '+%Y-%m-%d')

    log ""
    log "── 第 $ROUND 轮扫描 [$START_DATE → $END_DATE] ──────────────"

    $PYTHON manual_historical_backfill.py \
        -s "$STOCK_LIST_FILE" \
        --date-range "$START_DATE" "$END_DATE" \
        >> "$LOG_FILE" 2>&1 \
        && log "✅ 第 $ROUND 轮扫描完成" \
        || log "⚠️  第 $ROUND 轮扫描出错（见上方日志）"

    # 检查是否还在时间窗口内，若是则休眠30分钟再扫描
    if is_within_window; then
        log "😴 等待 30 分钟后进行下一轮扫描..."
        sleep 1800
    fi
done

log ""
log "======================================================"
log "⏰ 已超过 HKT 07:00，夜间任务结束"
log "======================================================"

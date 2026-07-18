# 从旧版升级到 hybrid（Redis + PostgreSQL）

当前版本默认 **高并发 hybrid 模式**：

- **PostgreSQL**：账号凭证、API Key、设置、账号池状态（含冷却）
- **Redis**：粘性会话、热计数、轮询游标、维护锁、管理会话
- `data/*.json` **仅作迁移源与管理台导入导出**，运行时不再写本地 JSON 镜像

---

## 场景 A：旧版仅文件后端（`data/auth.json` 等）→ 新版 hybrid

### 1. 备份

```bash
# 备份旧 data 目录
cp -a ./data ./data.backup-$(date +%Y%m%d)

# 若已有 PostgreSQL，也请先备份
# pg_dump "$DATABASE_URL" > pg-backup-$(date +%Y%m%d).sql
```

### 2. 配置环境

```bash
cp .env.example .env
# 至少设置：
# GROK2API_ADMIN_PASSWORD=...
# REDIS_URL=...
# DATABASE_URL=...
# GROK2API_STORE_BACKEND=hybrid
# GROK2API_REQUIRE_SHARED_STORES=1
```

### 3. 启动依赖并迁移

**推荐（包装脚本）：**

```bash
chmod +x scripts/upgrade_from_file_backend.sh
./scripts/upgrade_from_file_backend.sh --data-dir ./data
```

**或手动：**

```bash
docker compose up -d redis postgres
# postgres/redis 默认不映射宿主机端口；迁移容器走 compose 内网服务名
# 等待 postgres healthy 后：
docker compose run --rm \
  -e DATABASE_URL=postgresql://grok2api:grok2api@postgres:5432/grok2api \
  grokcli-2api \
  python scripts/migrate_json_to_pg.py --data-dir /app/data --merge-pool
# 根目录 `python migrate_json_to_pg.py` 仍可用（兼容包装）
```

本地非 Docker（需本机已有可连的 Postgres，或临时在 override 里映射端口）：

```bash
pip install -r requirements.txt -r requirements-store.txt
export DATABASE_URL=postgresql://grok2api:grok2api@127.0.0.1:5432/grok2api
python scripts/migrate_json_to_pg.py --data-dir ./data --merge-pool
# 或：python migrate_json_to_pg.py --data-dir ./data --merge-pool
```

### 4. 启动应用

```bash
docker compose up -d
curl -fsS http://127.0.0.1:3000/health
```

### 5. 验证

- 管理台账号数量与迁移前一致
- API Key 仍可访问 `/v1/models`
- 冷却/启用状态在账号列表可见（来自 `account_pool`）

### 迁移范围

| 迁入 PostgreSQL | 不迁移 |
|-----------------|--------|
| `auth.json` → `accounts` | Redis 热计数 / 粘性（可空启动） |
| `keys.json` → `api_keys` | 管理台登录会话（需重新登录） |
| `settings.json` 标量 / 注册配置 / 管理员密码哈希 | 审计日志历史（若旧版无表） |
| `settings.json` 内 `account_pool` | — |
| `models_cache.json` → `models` 表（一次性迁移，可 `--skip-models`） | 运行时不再使用 `models_cache.json` |

### 注意

- **首次迁移不要多实例并发跑** migrator；迁移完成后再拉高 `GROK2API_WORKERS`
- 使用 `--merge-pool` 可在 PG 已有数据时合并，避免误清空
- `keys` 导入为 **整表替换**（`replace_all`）；若 PG 里已有 Key 且 JSON 不全，请先备份
- 迁移完成后 hybrid 运行时 **不会** 再写回 `auth.json` / `keys.json` / `settings.json` / `affinity.json`；备份请用管理台导出或 `pg_dump`

---

## 场景 B：已是 hybrid，仅升级应用版本

1. 备份 PostgreSQL
2. 拉取新镜像 / 源码并重建容器
3. 启动时 `grok2api/store/pg.py` 会 **幂等执行 schema ALTER**（无需再跑 JSON 迁移）
4. 检查 `/health` 与管理台

```bash
docker compose pull   # 若用 GHCR
# 或
docker compose build
docker compose up -d
curl -fsS http://127.0.0.1:3000/health
```

---

## 包结构迁移提示

真实实现已收敛到 `grok2api/` 包内：

- `grok2api/app.py`：FastAPI 应用主入口；根目录 `app.py` 只是兼容启动包装。
- `grok2api/store/`：Redis / PostgreSQL 存储层；根目录 `store/` 只是兼容包装。
- `grok2api/admin|pool|protocol|upstream/`：管理台、账号池、协议适配、上游集成。

旧脚本里的根导入仍暂时兼容，但新代码建议改成包路径：

```python
# old
import account_pool
from store.pg import connection

# new
from grok2api.pool import account_pool
from grok2api.store.pg import connection
```

---

## 回滚建议

- **文件时代**：保留 `data.backup-*`，可临时 `GROK2API_STORE_BACKEND=file` + `GROK2API_REQUIRE_SHARED_STORES=0` + `WORKERS=1` 应急（不推荐生产）
- **hybrid**：用 `pg_dump` 备份恢复；Redis 可丢（热状态）

---

## 相关命令速查

```bash
# dry-run 查看将导入什么（推荐路径）
python scripts/migrate_json_to_pg.py --data-dir ./data --dry-run

# 只导入账号，跳过 keys
python scripts/migrate_json_to_pg.py --data-dir ./data --skip-keys --merge-pool

# 根目录包装仍兼容旧命令
python migrate_json_to_pg.py --data-dir ./data --dry-run

# 健康与存储
curl -s http://127.0.0.1:3000/health | jq .
curl -s http://127.0.0.1:3000/metrics | head
```


---

## 场景 C：Go 2.x 空库 / `schema_migrations does not exist`

Go 主进程**不会**在启动时改 schema，只校验 `schema_migrations`。Docker ≥2.0.1 入口会自动跑 `grok2api-migrate up`。

手工恢复（兼容旧库，`IF NOT EXISTS` 不删数据）：

```bash
# 备份
docker exec grokcli-2api-postgres pg_dump -U grok2api -d grok2api \
  > /root/grok2api-before-migration-$(date +%F-%H%M%S).sql

# 迁移 + 校验
docker exec grokcli-2api /app/bin/grok2api-migrate -dir /app/migrations up
docker exec grokcli-2api /app/bin/grok2api-migrate -dir /app/migrations verify

# 重启
docker restart grokcli-2api
curl -fsS http://127.0.0.1:3000/health || curl -fsS http://127.0.0.1:40081/health
```

新部署用 `docker compose up -d` 即可；入口默认 `GROK2API_AUTO_MIGRATE=1`。

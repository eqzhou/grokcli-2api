import time
import asyncio

# 内存数据库，用于临时存储验证码结果
results_db = {}

# Terminal results older than this are eligible for short-TTL cleanup (seconds).
TERMINAL_TTL_SEC = 3600  # 1 hour
TERMINAL_VALUES = frozenset({"CAPTCHA_FAIL", "CAPTCHA_NOT_READY"})


async def init_db():
    print("[系统] 结果数据库初始化成功 (内存模式)")


async def save_result(task_id, task_type, data):
    # Preserve createTime across status updates so cleanup can age terminal rows.
    prev = results_db.get(task_id) if isinstance(results_db.get(task_id), dict) else None
    if not isinstance(data, dict):
        data = {"value": data}
    else:
        data = dict(data)
    create_time = None
    if prev is not None:
        create_time = prev.get("createTime")
    if create_time is None:
        create_time = data.get("createTime")
    if create_time is None:
        create_time = int(time.time())
    data["createTime"] = int(create_time)
    if "task_type" not in data and task_type:
        data["task_type"] = task_type
    results_db[task_id] = data
    print(f"[系统] 任务 {task_id} 状态更新: {data.get('value', '正在处理')}")


async def load_result(task_id):
    return results_db.get(task_id)


async def cleanup_old_results(days_old=7, terminal_ttl_sec=None):
    """Drop aged results.

    - Any result older than days_old (via createTime) is removed.
    - Terminal results (success token / CAPTCHA_FAIL) older than terminal_ttl_sec
      are removed even if createTime is missing (treat missing as now-ageable).
    """
    now = time.time()
    ttl_long = max(1.0, float(days_old) * 86400.0)
    try:
        term_ttl = float(
            terminal_ttl_sec
            if terminal_ttl_sec is not None
            else TERMINAL_TTL_SEC
        )
    except (TypeError, ValueError):
        term_ttl = float(TERMINAL_TTL_SEC)
    term_ttl = max(60.0, term_ttl)

    to_delete = []
    for tid, res in list(results_db.items()):
        if not isinstance(res, dict):
            to_delete.append(tid)
            continue
        created = res.get("createTime")
        try:
            created_f = float(created) if created is not None else None
        except (TypeError, ValueError):
            created_f = None
        value = str(res.get("value") or "")
        # Non-pending values are terminal (token string or CAPTCHA_FAIL).
        is_terminal = value not in ("", "CAPTCHA_NOT_READY") or value == "CAPTCHA_FAIL"
        if value == "CAPTCHA_NOT_READY":
            is_terminal = False
        elif value:
            is_terminal = True

        age = (now - created_f) if created_f is not None else None
        if age is not None and age > ttl_long:
            to_delete.append(tid)
            continue
        if is_terminal:
            # Terminal without createTime: treat as immediately ageable under short TTL
            # using a synthetic age of term_ttl+ so they cannot pin memory forever.
            if age is None or age > term_ttl:
                to_delete.append(tid)
                continue
    for tid in to_delete:
        results_db.pop(tid, None)
    return len(to_delete)

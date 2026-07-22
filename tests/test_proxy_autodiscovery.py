from grok2api.upstream import proxy_pool


def test_host_runtime_does_not_probe_container_proxy_names(monkeypatch) -> None:
    for key in (
        "GROK2API_AUTO_PROXY",
        "HTTPS_PROXY",
        "HTTP_PROXY",
        "https_proxy",
        "http_proxy",
        "ALL_PROXY",
        "all_proxy",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("GROK2API_CONTAINERIZED", raising=False)
    monkeypatch.setattr(proxy_pool.os.path, "exists", lambda path: False)

    candidates = proxy_pool._auto_proxy_candidates()

    assert candidates == []
    assert all("host.docker.internal" not in item for item in candidates)

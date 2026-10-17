import pytest

from payment_service.config import get_settings
from payment_service.messaging.topology import (
    ROUTING_KEY_NEW,
    all_queue_names,
    new_queue,
    retry_queue_names,
)

pytestmark = pytest.mark.unit


def test_new_queue_routing_matches_routing_key_new() -> None:
    assert new_queue().routing() == ROUTING_KEY_NEW


def apply_topology_env(monkeypatch: pytest.MonkeyPatch, ttls: str) -> None:
    monkeypatch.setenv("API_KEYS", "unit")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_USER", "payments")
    monkeypatch.setenv("DB_PASSWORD", "payments")
    monkeypatch.setenv("DB_NAME", "payments")
    monkeypatch.setenv("RETRY_TTLS_MS", ttls)


def test_retry_queue_names_follow_ttl_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    apply_topology_env(monkeypatch, "100,200,300,400")
    get_settings.cache_clear()
    settings = get_settings()
    assert retry_queue_names(settings) == [
        "payments.retry.1",
        "payments.retry.2",
        "payments.retry.3",
        "payments.retry.4",
    ]


def test_all_queue_names_covers_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    apply_topology_env(monkeypatch, "100,200")
    monkeypatch.setenv("RETRY_MAX_ATTEMPTS", "2")
    get_settings.cache_clear()
    settings = get_settings()
    assert all_queue_names(settings) == [
        "payments.new",
        "payments.retry.1",
        "payments.retry.2",
        "payments.dlq",
    ]


def test_all_queue_names_defaults_to_three_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    apply_topology_env(monkeypatch, "5000,10000,20000")
    get_settings.cache_clear()
    settings = get_settings()
    names = all_queue_names(settings)
    assert names[0] == "payments.new"
    assert names[-1] == "payments.dlq"
    assert len(names) == 5

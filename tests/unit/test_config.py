import pytest
from pydantic import ValidationError

from payment_service.config import (
    ApiSettings,
    DbSettings,
    LogSettings,
    OutboxSettings,
    ProcessingSettings,
    RabbitSettings,
    WebhookSettings,
    get_settings,
)

pytestmark = pytest.mark.unit


def apply_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_KEYS", "required-key")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_USER", "payments")
    monkeypatch.setenv("DB_PASSWORD", "required-password")
    monkeypatch.setenv("DB_NAME", "payments")


def test_env_to_settings_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_KEYS", "a,b")
    api = ApiSettings()
    assert api.api_keys == ["a", "b"]


def test_empty_api_keys_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_KEYS", "")
    with pytest.raises(ValidationError):
        ApiSettings()


def test_blank_api_keys_entries_filtered() -> None:
    api = ApiSettings(API_KEYS="a,,b")
    assert api.api_keys == ["a", "b"]


def test_api_keys_csv_without_gaps() -> None:
    api = ApiSettings(API_KEYS="key1,key2")
    assert api.api_keys == ["key1", "key2"]


def test_docs_enabled_defaults_to_true(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_KEYS", "k")
    api = ApiSettings()
    assert api.docs_enabled is True


def test_docs_enabled_disabled_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_KEYS", "k")
    monkeypatch.setenv("DOCS_ENABLED", "false")
    api = ApiSettings()
    assert api.docs_enabled is False


def test_dsn_assembles() -> None:
    db = DbSettings(host="db", user="u", password="p", name="n")
    assert db.dsn == "postgresql+asyncpg://u:p@db:5432/n"


def test_processing_defaults() -> None:
    processing = ProcessingSettings()
    assert processing.success_rate == 0.9
    assert processing.delay_min_s == 2.0
    assert processing.delay_max_s == 5.0
    assert processing.retry_ttls_ms == [5000, 10000, 20000]
    assert processing.retry_max_attempts == 3


def test_processing_success_rate_out_of_range() -> None:
    with pytest.raises(ValidationError):
        ProcessingSettings(success_rate=1.5)


def test_retry_ttls_from_env_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RETRY_TTLS_MS", "1000,1000,1000")
    processing = ProcessingSettings()
    assert processing.retry_ttls_ms == [1000, 1000, 1000]


def test_retry_ttls_csv_with_blank_entries_filtered() -> None:
    processing = ProcessingSettings(RETRY_TTLS_MS="5000,10000", RETRY_MAX_ATTEMPTS=2)
    assert processing.retry_ttls_ms == [5000, 10000]


def test_webhook_secret_blank_normalizes_to_none() -> None:
    assert WebhookSettings(secret="").secret is None


def test_webhook_secret_whitespace_normalizes_to_none() -> None:
    assert WebhookSettings(secret="   ").secret is None


def test_webhook_secret_non_blank_preserved() -> None:
    assert WebhookSettings(secret="abc").secret == "abc"


def test_webhook_secret_blank_env_normalizes_to_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WEBHOOK_SECRET", "")
    assert WebhookSettings().secret is None


def test_outbox_defaults() -> None:
    outbox = OutboxSettings()
    assert outbox.retention_s == 86400.0
    assert outbox.cleanup_interval_s == 3600.0


def test_outbox_retention_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OUTBOX_RETENTION_S", "60")
    monkeypatch.setenv("OUTBOX_CLEANUP_INTERVAL_S", "5")
    outbox = OutboxSettings()
    assert outbox.retention_s == 60.0
    assert outbox.cleanup_interval_s == 5.0


def test_outbox_retention_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        OutboxSettings(retention_s=0)


def test_log_level_defaults_and_normalization() -> None:
    log = LogSettings()
    assert log.level == "INFO"
    assert LogSettings(level="debug").level == "DEBUG"


def test_log_level_rejects_unknown() -> None:
    with pytest.raises(ValidationError):
        LogSettings(level="VERBOSE")


def test_log_level_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOG_LEVEL", "warning")
    assert LogSettings().level == "WARNING"


def test_get_settings_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    apply_required_env(monkeypatch)
    monkeypatch.setenv("API_KEYS", "first")
    first = get_settings()
    assert get_settings() is first
    get_settings.cache_clear()
    monkeypatch.setenv("API_KEYS", "second")
    second = get_settings()
    assert second is not first
    assert second.api.api_keys == ["second"]


def test_missing_db_password_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_USER", "payments")
    monkeypatch.setenv("DB_NAME", "payments")
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    with pytest.raises(ValidationError):
        DbSettings()


def test_api_settings_do_not_require_rabbit_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    apply_required_env(monkeypatch)
    monkeypatch.delenv("RABBIT_URL", raising=False)
    settings = get_settings()
    assert settings.api.api_keys == ["required-key"]


def test_rabbit_settings_requires_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RABBIT_URL", raising=False)
    with pytest.raises(ValidationError):
        RabbitSettings()


def test_retry_max_attempts_cannot_exceed_ttl_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RETRY_MAX_ATTEMPTS", "5")
    with pytest.raises(ValidationError):
        ProcessingSettings()


def test_retry_settings_consistent_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RETRY_TTLS_MS", "1000,1000,1000")
    monkeypatch.setenv("RETRY_MAX_ATTEMPTS", "3")
    processing = ProcessingSettings()
    assert processing.retry_max_attempts == 3
    assert processing.retry_ttls_ms == [1000, 1000, 1000]

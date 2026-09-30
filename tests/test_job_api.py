from datetime import UTC, datetime

from app.metrics import jobs_enqueued_total
from app.models.snapshots import ChainSnapshot, SnapshotRun, WalletSnapshot


def test_internal_token_required(client):
    response = client.post(
        "/internal/snapshot-jobs",
        json={"user_id": 1, "trigger_type": "manual", "scope_type": "all"},
    )

    assert response.status_code == 401


def test_invalid_internal_token_rejected(client):
    response = client.post(
        "/internal/snapshot-jobs",
        headers={"X-Internal-Token": "wrong-token"},
        json={"user_id": 1, "trigger_type": "manual", "scope_type": "all"},
    )

    assert response.status_code == 401


def test_can_create_all_job(client):
    enqueued_before = jobs_enqueued_total.labels("api", "manual", "all")._value.get()
    response = client.post(
        "/internal/snapshot-jobs",
        headers={"X-Internal-Token": "test-token"},
        json={"user_id": 1, "trigger_type": "manual", "scope_type": "all"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "pending"
    assert response.json()["reused"] is False
    assert jobs_enqueued_total.labels("api", "manual", "all")._value.get() == enqueued_before + 1


def test_active_job_is_reused_for_the_same_user_and_scope(client, db_session):
    enqueued_before = jobs_enqueued_total.labels("api", "manual", "all")._value.get()
    first = client.post(
        "/internal/snapshot-jobs",
        headers={"X-Internal-Token": "test-token"},
        json={"user_id": 1, "trigger_type": "manual", "scope_type": "all"},
    )
    second = client.post(
        "/internal/snapshot-jobs",
        headers={"X-Internal-Token": "test-token"},
        json={"user_id": 1, "trigger_type": "manual", "scope_type": "all"},
    )

    assert second.status_code == 200
    assert second.json() == {
        "job_id": first.json()["job_id"],
        "status": "pending",
        "reused": True,
    }
    assert db_session.query(SnapshotRun).count() == 1
    assert jobs_enqueued_total.labels("api", "manual", "all")._value.get() == enqueued_before + 1


def test_active_job_reuse_is_scoped_by_user_and_target(client, db_session):
    all_job = client.post(
        "/internal/snapshot-jobs",
        headers={"X-Internal-Token": "test-token"},
        json={"user_id": 1, "trigger_type": "manual", "scope_type": "all"},
    )
    wallet_job = client.post(
        "/internal/snapshot-jobs",
        headers={"X-Internal-Token": "test-token"},
        json={"user_id": 1, "trigger_type": "manual", "scope_type": "wallet", "wallet_id": 25},
    )
    other_user_job = client.post(
        "/internal/snapshot-jobs",
        headers={"X-Internal-Token": "test-token"},
        json={"user_id": 2, "trigger_type": "manual", "scope_type": "all"},
    )

    assert all_job.json()["reused"] is False
    assert wallet_job.json()["reused"] is False
    assert other_user_job.json()["reused"] is False
    assert db_session.query(SnapshotRun).count() == 3


def test_can_get_job_status(client):
    created = client.post(
        "/internal/snapshot-jobs",
        headers={"X-Internal-Token": "test-token"},
        json={"user_id": 1, "trigger_type": "manual", "scope_type": "all"},
    )
    job_id = created.json()["job_id"]

    response = client.get(
        f"/internal/snapshot-jobs/{job_id}",
        headers={"X-Internal-Token": "test-token"},
    )

    assert response.status_code == 200
    assert response.json()["job_id"] == job_id
    assert response.json()["status"] == "pending"


def test_get_job_status_returns_not_found_for_missing_job(client):
    response = client.get(
        "/internal/snapshot-jobs/999",
        headers={"X-Internal-Token": "test-token"},
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "job not found"}


def test_can_create_group_job(client):
    response = client.post(
        "/internal/snapshot-jobs",
        headers={"X-Internal-Token": "test-token"},
        json={"user_id": 1, "trigger_type": "manual", "scope_type": "group", "group_id": 10},
    )

    assert response.status_code == 200


def test_can_create_wallet_job(client):
    response = client.post(
        "/internal/snapshot-jobs",
        headers={"X-Internal-Token": "test-token"},
        json={"user_id": 1, "trigger_type": "manual", "scope_type": "wallet", "wallet_id": 25},
    )

    assert response.status_code == 200


def test_first_wallet_job_persists_bounded_activation_channel(client, db_session):
    response = client.post(
        "/internal/snapshot-jobs",
        headers={"X-Internal-Token": "test-token"},
        json={
            "user_id": 1,
            "trigger_type": "auto",
            "scope_type": "wallet",
            "wallet_id": 25,
            "activation_channel": "telegram",
        },
    )

    assert response.status_code == 200
    assert db_session.query(SnapshotRun).one().activation_channel == "telegram"


def test_activation_channel_rejects_unbounded_or_non_auto_values(client):
    unbounded = client.post(
        "/internal/snapshot-jobs",
        headers={"X-Internal-Token": "test-token"},
        json={
            "user_id": 1,
            "trigger_type": "auto",
            "scope_type": "wallet",
            "wallet_id": 25,
            "activation_channel": "user-123",
        },
    )
    manual = client.post(
        "/internal/snapshot-jobs",
        headers={"X-Internal-Token": "test-token"},
        json={
            "user_id": 1,
            "trigger_type": "manual",
            "scope_type": "wallet",
            "wallet_id": 25,
            "activation_channel": "web",
        },
    )

    assert unbounded.status_code == 422
    assert manual.status_code == 422


def test_invalid_scope_validation(client):
    response = client.post(
        "/internal/snapshot-jobs",
        headers={"X-Internal-Token": "test-token"},
        json={"user_id": 1, "trigger_type": "manual", "scope_type": "group"},
    )

    assert response.status_code == 422


def test_retry_failed_job_reuses_active_child(client, db_session):
    parent = SnapshotRun(
        user_id=1,
        trigger_type="manual",
        scope_type="all",
        status="partial_success",
        created_at=datetime.now(UTC),
    )
    db_session.add(parent)
    db_session.flush()
    wallet_snapshot = WalletSnapshot(
        snapshot_run_id=parent.id,
        wallet_id=1,
        wallet_type="evm",
        status="partial_success",
        total_usd=0,
    )
    db_session.add(wallet_snapshot)
    db_session.flush()
    db_session.add(
        ChainSnapshot(
            wallet_snapshot_id=wallet_snapshot.id,
            chain="base",
            status="failed",
            total_usd=0,
            error_type="timeout",
        )
    )
    db_session.commit()

    first = client.post(
        f"/internal/snapshot-jobs/{parent.id}/retry-failed",
        headers={"X-Internal-Token": "test-token"},
    )
    second = client.post(
        f"/internal/snapshot-jobs/{parent.id}/retry-failed",
        headers={"X-Internal-Token": "test-token"},
    )

    assert first.status_code == 200
    assert first.json()["reused"] is False
    assert second.status_code == 200
    assert second.json() == {
        "job_id": first.json()["job_id"],
        "status": "pending",
        "reused": True,
    }
    assert db_session.query(SnapshotRun).filter(SnapshotRun.parent_run_id == parent.id).count() == 1


def test_retry_rejects_nonterminal_parent(client, db_session):
    parent = SnapshotRun(
        user_id=1,
        trigger_type="manual",
        scope_type="all",
        status="running",
        created_at=datetime.now(UTC),
    )
    db_session.add(parent)
    db_session.commit()

    response = client.post(
        f"/internal/snapshot-jobs/{parent.id}/retry-failed",
        headers={"X-Internal-Token": "test-token"},
    )

    assert response.status_code == 409


def test_retry_returns_not_found_for_missing_parent(client):
    response = client.post(
        "/internal/snapshot-jobs/999/retry-failed",
        headers={"X-Internal-Token": "test-token"},
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "parent job not found"}


def test_retry_rejects_terminal_parent_without_failed_chains(client, db_session):
    parent = SnapshotRun(
        user_id=1,
        trigger_type="manual",
        scope_type="all",
        status="partial_success",
        created_at=datetime.now(UTC),
    )
    db_session.add(parent)
    db_session.commit()

    response = client.post(
        f"/internal/snapshot-jobs/{parent.id}/retry-failed",
        headers={"X-Internal-Token": "test-token"},
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "parent job has no failed chains"}
    assert db_session.query(SnapshotRun).count() == 1

"""Tests du back-office administrateur."""


def test_admin_interdit_aux_non_admins(auth_client):
    assert auth_client.get("/api/admin/stats").status_code == 403


def test_admin_accessible_a_un_admin(client):
    from app.database import SessionLocal
    from app.models import User
    from app.security import hash_password

    with SessionLocal() as db:
        db.add(User(email="boss@x.com", hashed_password=hash_password("password123"), is_admin=True))
        db.commit()

    token = client.post(
        "/api/auth/login", data={"username": "boss@x.com", "password": "password123"}
    ).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    stats = client.get("/api/admin/stats", headers=headers)
    assert stats.status_code == 200
    assert "users" in stats.json() and "active_subscriptions" in stats.json()

    users = client.get("/api/admin/users", headers=headers)
    assert users.status_code == 200
    assert any(u["email"] == "boss@x.com" for u in users.json())

from __future__ import annotations

API = "/api/v1"


def _new_alert(client, h, make_events, scenario="account_takeover"):
    """Return alert ids raised for the scenario's TARGET customer only (other customers' normal
    traffic can occasionally be flagged too; investigations group alerts of one customer)."""
    for _ in range(5):
        evs = make_events(scenario, n_normal=12)
        target = next(e.customer_id for e in reversed(evs) if e.ground_truth.is_suspicious)
        uids = []
        for ev in evs:
            r = client.post(f"{API}/transactions/ingest", headers=h, json=ev.to_wire()).json()
            if r.get("alert_uid") and ev.customer_id == target:
                uids.append(r["alert_uid"])
        if uids:
            return uids
    raise AssertionError("could not produce an alert")


def test_alert_lifecycle_notes_and_full_audit_trail(client, analyst_h, make_events):
    uid = _new_alert(client, analyst_h, make_events)[-1]
    r = client.patch(f"{API}/alerts/{uid}/status", headers=analyst_h, json={"status": "UNDER_REVIEW"})
    assert r.status_code == 200 and r.json()["assigned_to"]["username"] == "analyst"
    assert client.post(f"{API}/alerts/{uid}/notes", headers=analyst_h, json={"body": "Device never seen before."}).status_code == 201
    r = client.patch(f"{API}/alerts/{uid}/status", headers=analyst_h,
                     json={"status": "CONFIRMED_SUSPICIOUS", "note": "Pattern consistent with takeover."})
    assert r.json()["status"] == "CONFIRMED_SUSPICIOUS"
    a = client.get(f"{API}/alerts/{uid}", headers=analyst_h).json()
    assert [n["author"] for n in a["notes"]] == ["analyst", "analyst"]
    changes = [(x["action"], x["field"], x["old_value"], x["new_value"], x["actor"]) for x in a["audit"]]
    assert ("ALERT_CREATED", "status", None, "NEW", "system") in changes
    assert ("ALERT_STATUS_CHANGED", "status", "NEW", "UNDER_REVIEW", "analyst") in changes
    assert ("ALERT_STATUS_CHANGED", "status", "UNDER_REVIEW", "CONFIRMED_SUSPICIOUS", "analyst") in changes
    assert all(x["created_at"] for x in a["audit"])


def test_illegal_transition_rejected(client, analyst_h, make_events):
    uid = _new_alert(client, analyst_h, make_events)[-1]
    r = client.patch(f"{API}/alerts/{uid}/status", headers=analyst_h, json={"status": "CONFIRMED_SUSPICIOUS"})
    assert r.status_code == 409      # NEW -> CONFIRMED requires review first
    assert client.patch(f"{API}/alerts/{uid}/status", headers=analyst_h, json={"status": "BOGUS"}).status_code == 422


def test_analyst_restrictions_and_admin_override(client, analyst_h, analyst2_h, admin_h, make_events):
    uid = _new_alert(client, analyst_h, make_events)[-1]
    client.patch(f"{API}/alerts/{uid}/status", headers=analyst_h, json={"status": "UNDER_REVIEW"})
    # another analyst cannot act on an alert assigned to someone else
    assert client.patch(f"{API}/alerts/{uid}/status", headers=analyst2_h, json={"status": "DISMISSED"}).status_code == 403
    users = {u["username"]: u["id"] for u in client.get(f"{API}/users", headers=analyst_h).json()}
    assert client.post(f"{API}/alerts/{uid}/assign", headers=analyst_h, json={"user_id": users["analyst2"]}).status_code == 403
    assert client.post(f"{API}/alerts/{uid}/assign", headers=admin_h, json={"user_id": users["analyst2"]}).status_code == 200
    assert client.patch(f"{API}/alerts/{uid}/status", headers=analyst2_h, json={"status": "DISMISSED"}).status_code == 200
    assert client.patch(f"{API}/alerts/{uid}/status", headers=analyst2_h, json={"status": "CLOSED"}).status_code == 200
    # only admins may re-open closed alerts
    assert client.patch(f"{API}/alerts/{uid}/status", headers=analyst2_h, json={"status": "UNDER_REVIEW"}).status_code == 403
    assert client.patch(f"{API}/alerts/{uid}/status", headers=admin_h, json={"status": "UNDER_REVIEW"}).status_code == 200


def test_investigation_workflow_end_to_end(client, analyst_h, admin_h, make_events):
    uids = _new_alert(client, analyst_h, make_events)
    r = client.post(f"{API}/investigations", headers=analyst_h,
                    json={"alert_uids": uids, "title": "Possible account takeover", "priority": "HIGH"})
    assert r.status_code == 201, r.text
    inv = r.json()
    case = inv["case_uid"]
    assert inv["status"] == "OPEN" and inv["alert_count"] == len(uids)
    assert all(a["status"] == "UNDER_REVIEW" for a in inv["alerts"])
    # re-linking the same alert to another case is rejected
    assert client.post(f"{API}/investigations", headers=analyst_h,
                       json={"alert_uids": uids[:1], "title": "dup"}).status_code == 409
    assert client.post(f"{API}/investigations/{case}/notes", headers=analyst_h,
                       json={"body": "Customer history reviewed; new device + far location."}).status_code == 201
    assert client.patch(f"{API}/investigations/{case}/status", headers=analyst_h, json={"status": "IN_PROGRESS"}).status_code == 200
    bad = client.patch(f"{API}/investigations/{case}/status", headers=analyst_h, json={"status": "RESOLVED"})
    assert bad.status_code == 422      # findings & classification required
    r = client.patch(f"{API}/investigations/{case}/status", headers=analyst_h,
                     json={"status": "RESOLVED", "final_classification": "CONFIRMED_SUSPICIOUS",
                           "findings": "Synthetic takeover pattern confirmed by analyst review."})
    assert r.status_code == 200
    body = r.json()
    assert body["final_classification"] == "CONFIRMED_SUSPICIOUS"
    assert all(a["status"] == "CONFIRMED_SUSPICIOUS" for a in body["alerts"])
    assert client.patch(f"{API}/investigations/{case}/status", headers=analyst_h, json={"status": "CLOSED"}).status_code == 200
    assert client.patch(f"{API}/investigations/{case}/status", headers=analyst_h, json={"status": "IN_PROGRESS"}).status_code == 403
    detail = client.get(f"{API}/investigations/{case}", headers=analyst_h).json()
    actions = [a["action"] for a in detail["audit"]]
    for expected in ("INVESTIGATION_OPENED", "NOTE_ADDED", "INVESTIGATION_FINDINGS", "INVESTIGATION_STATUS_CHANGED"):
        assert expected in actions
    assert detail["customer_transactions"] and detail["notes"][0]["author"] == "analyst"
    assert detail["closed_at"] is not None
    lst = client.get(f"{API}/investigations?status=CLOSED", headers=analyst_h).json()
    assert any(i["case_uid"] == case for i in lst["items"])
    cust = client.get(f"{API}/customers/{detail['customer_id']}", headers=analyst_h).json()
    assert cust["transaction_count"] > 0 and cust["feature_state"]["total_count"] > 0


def test_alerts_list_filters_search_sort_and_pagination(client, analyst_h, make_events):
    _new_alert(client, analyst_h, make_events, "velocity_burst")
    r = client.get(f"{API}/alerts?size=3&sort=risk_score&order=desc", headers=analyst_h).json()
    assert r["total"] >= 1 and len(r["items"]) <= 3
    s = [a["risk_score"] for a in r["items"]]
    assert s == sorted(s, reverse=True)
    new = client.get(f"{API}/alerts?status=NEW", headers=analyst_h).json()
    assert all(a["status"] == "NEW" for a in new["items"])
    cid = r["items"][0]["customer_id"]
    q = client.get(f"{API}/alerts?q={cid}", headers=analyst_h).json()
    assert q["total"] >= 1 and all(a["customer_id"] == cid for a in q["items"])
    mine = client.get(f"{API}/alerts?assigned=me", headers=analyst_h).json()
    assert all(a["assigned_to"]["username"] == "analyst" for a in mine["items"])
    summ = client.get(f"{API}/alerts/summary", headers=analyst_h).json()
    assert sum(summ["by_status"].values()) >= r["total"] - 0
    assert client.get(f"{API}/alerts/does-not-exist", headers=analyst_h).status_code == 404

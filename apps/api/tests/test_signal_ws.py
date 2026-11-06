"""The signaling WebSocket: room creation, joining, signal relay, and leave notification."""

import json

from fastapi.testclient import TestClient


def _recv(ws) -> dict:
    return json.loads(ws.receive_text())


def test_create_join_and_signal_relay(client: TestClient) -> None:
    with client.websocket_connect("/api/ws") as host, client.websocket_connect("/api/ws") as guest:
        host.send_text(json.dumps({"cmd": "create"}))
        created = _recv(host)
        assert created["cmd"] == "created"
        room = created["room"]
        assert len(room) == 5

        guest.send_text(json.dumps({"cmd": "join", "room": room}))
        assert _recv(guest) == {"cmd": "joined", "room": room}
        assert _recv(host) == {"cmd": "peer"}

        host.send_text(json.dumps({"cmd": "signal", "data": {"sdp": {"type": "offer"}}}))
        assert _recv(guest) == {"cmd": "signal", "data": {"sdp": {"type": "offer"}}}

        guest.send_text(json.dumps({"cmd": "signal", "data": {"candidate": "candidate:1"}}))
        assert _recv(host) == {"cmd": "signal", "data": {"candidate": "candidate:1"}}

    with client.websocket_connect("/api/ws") as late:
        late.send_text(json.dumps({"cmd": "join", "room": room}))
        assert _recv(late)["cmd"] == "error"


def test_join_without_room_is_an_error(client: TestClient) -> None:
    with client.websocket_connect("/api/ws") as ws:
        ws.send_text(json.dumps({"cmd": "join", "room": "ZZZZZ"}))
        assert _recv(ws)["cmd"] == "error"


def test_leaving_notifies_the_peer(client: TestClient) -> None:
    with client.websocket_connect("/api/ws") as host:
        host.send_text(json.dumps({"cmd": "create"}))
        room = _recv(host)["room"]
        with client.websocket_connect("/api/ws") as guest:
            guest.send_text(json.dumps({"cmd": "join", "room": room}))
            _recv(guest)
            _recv(host)
        assert _recv(host) == {"cmd": "peer-left"}


def test_a_full_room_refuses_a_third_peer(client: TestClient) -> None:
    with client.websocket_connect("/api/ws") as host, client.websocket_connect("/api/ws") as guest:
        host.send_text(json.dumps({"cmd": "create"}))
        room = _recv(host)["room"]
        guest.send_text(json.dumps({"cmd": "join", "room": room}))
        _recv(guest)
        _recv(host)
        with client.websocket_connect("/api/ws") as third:
            third.send_text(json.dumps({"cmd": "join", "room": room}))
            assert _recv(third)["cmd"] == "error"


def test_the_app_still_serves_its_routes(client: TestClient) -> None:
    res = client.get("/api/health")
    assert res.status_code == 200
    with client.websocket_connect("/api/ws") as ws:
        ws.send_text(json.dumps({"cmd": "create"}))
        assert _recv(ws)["cmd"] == "created"

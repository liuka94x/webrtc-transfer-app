"""The signaling WebSocket at ``/api/ws``.

Two peers per room: the first ``create``s a room and gets its code, the second ``join``s it; from
then on each ``signal`` message (SDP offer/answer or an ICE candidate) is relayed to the other
peer. File contents never pass through here: once WebRTC connects, peers talk directly.
"""

import json
import secrets

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()

_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

rooms: dict[str, dict[str, WebSocket]] = {}


def _new_code() -> str:
    while True:
        code = "".join(secrets.choice(_CODE_ALPHABET) for _ in range(5))
        if code not in rooms:
            return code


async def _relay(members: dict[str, WebSocket], sender: str, message: dict) -> None:
    payload = json.dumps(message)
    for peer, websocket in list(members.items()):
        if peer == sender:
            continue
        try:
            await websocket.send_text(payload)
        except Exception:  # noqa: BLE001 -- a dead peer must not break the other one's room
            pass


@router.websocket("")
async def signal(websocket: WebSocket) -> None:
    await websocket.accept()
    peer = secrets.token_hex(8)
    room: str | None = None
    try:
        while True:
            try:
                message = json.loads(await websocket.receive_text())
            except WebSocketDisconnect:
                break
            except json.JSONDecodeError:
                continue
            cmd = message.get("cmd")
            if cmd == "create" and room is None:
                room = _new_code()
                rooms[room] = {peer: websocket}
                await websocket.send_text(json.dumps({"cmd": "created", "room": room}))
            elif cmd == "join" and room is None:
                code = str(message.get("room", "")).upper()
                members = rooms.get(code)
                if members is None or len(members) >= 2:
                    await websocket.send_text(
                        json.dumps({"cmd": "error", "message": "Stanza inesistente o piena"})
                    )
                else:
                    members[peer] = websocket
                    room = code
                    await websocket.send_text(json.dumps({"cmd": "joined", "room": code}))
                    await _relay(members, peer, {"cmd": "peer"})
            elif cmd == "signal" and room is not None:
                await _relay(rooms.get(room, {}), peer, {"cmd": "signal", "data": message.get("data")})
    finally:
        if room is not None:
            members = rooms.get(room)
            if members is not None:
                members.pop(peer, None)
                if members:
                    await _relay(members, peer, {"cmd": "peer-left"})
                else:
                    rooms.pop(room, None)

"""WebRTC signaling: WebSocket rooms of two peers relaying SDP and ICE candidates.

The gateway in front of the app authenticates the caller; the route itself needs no app-local
caller identity, so it declares an empty access rule like the infrastructure routes.
"""

from fastapi import params

segment = "ws"

dependencies: tuple[params.Depends, ...] = ()

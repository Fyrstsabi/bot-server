import asyncio
import json
import random
import string
import logging
import os
import websockets

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")

ROOM_CHARSET = "ABCDEFGHJKMNPQRSTVWXYZ"
rooms = {}
clients = set()

def generate_room_code():
    for _ in range(100):
        code = "".join(random.choice(ROOM_CHARSET) for _ in range(4))
        if code not in rooms:
            return code
    return "".join(random.choice(ROOM_CHARSET) for _ in range(6))

async def broadcast_counts():
    playing_count = sum(2 for r in rooms.values() if r.get('guest') is not None)
    online_count = len(clients)
    msg = json.dumps({
        "t": "count",
        "online": online_count,
        "playing": playing_count
    })
    for ws in list(clients):
        try:
            await ws.send(msg)
        except:
            pass

async def handler(websocket):
    clients.add(websocket)
    logging.info(f"Client connected. Total: {len(clients)}")
    current_room_code = None
    is_host = False

    try:
        await websocket.send(json.dumps({
            "t": "count",
            "online": len(clients),
            "playing": sum(2 for r in rooms.values() if r.get('guest') is not None)
        }))

        async for message in websocket:
            try:
                data = json.loads(message)
            except Exception:
                continue

            msg_type = data.get("t")

            # 1. Create Room
            if msg_type == "room_create":
                if current_room_code and current_room_code in rooms:
                    del rooms[current_room_code]

                code = generate_room_code()
                rooms[code] = {
                    "host": websocket,
                    "guest": None,
                    "host_token": "".join(random.choices(string.ascii_letters, k=16)),
                    "guest_token": "".join(random.choices(string.ascii_letters, k=16))
                }
                current_room_code = code
                is_host = True
                logging.info(f"Room created: {code}")
                await websocket.send(json.dumps({
                    "t": "room",
                    "code": code,
                    "token": rooms[code]["host_token"]
                }))
                await broadcast_counts()

            # 2. Join Room
            elif msg_type == "room_join":
                code = data.get("code", "").upper().strip()
                if code not in rooms:
                    await websocket.send(json.dumps({
                        "t": "error",
                        "why": f"ไม่พบห้อง {code}"
                    }))
                    continue

                room = rooms[code]
                if room["guest"] is not None:
                    await websocket.send(json.dumps({
                        "t": "error",
                        "why": f"ห้อง {code} เต็มแล้ว"
                    }))
                    continue

                room["guest"] = websocket
                current_room_code = code
                is_host = False
                logging.info(f"Player joined room: {code}")

                await room["host"].send(json.dumps({
                    "t": "paired",
                    "host": True,
                    "token": room["host_token"]
                }))
                await websocket.send(json.dumps({
                    "t": "paired",
                    "host": False,
                    "token": room["guest_token"]
                }))
                await broadcast_counts()

            # 3. Game action relay
            elif msg_type in ["game", "hello", "seed", "rules", "sync", "forfeit", "bye"]:
                if current_room_code and current_room_code in rooms:
                    room = rooms[current_room_code]
                    target = room["guest"] if is_host else room["host"]
                    if target:
                        await target.send(message)

            # 4. Leave room
            elif msg_type == "leave":
                if current_room_code and current_room_code in rooms:
                    room = rooms[current_room_code]
                    target = room["guest"] if is_host else room["host"]
                    if target:
                        await target.send(json.dumps({"t": "peer_left"}))
                    del rooms[current_room_code]
                    current_room_code = None
                    await broadcast_counts()

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clients.remove(websocket)
        logging.info(f"Client disconnected. Remaining: {len(clients)}")
        if current_room_code and current_room_code in rooms:
            room = rooms[current_room_code]
            target = room["guest"] if is_host else room["host"]
            if target:
                try:
                    await target.send(json.dumps({"t": "peer_lost"}))
                except:
                    pass
            del rooms[current_room_code]
        await broadcast_counts()

async def main():
    port = int(os.environ.get("PORT", 8765))
    server = await websockets.serve(handler, "0.0.0.0", port)
    logging.info(f"==================================================")
    logging.info(f"  BOT Custom Server running on port {port}")
    logging.info(f"==================================================")
    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())

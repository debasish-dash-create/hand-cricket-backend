from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uuid
import random
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, dict] = {} 
        # Separate queues for 1-over and 3-over quick matches
        self.quick_match_queue: dict[int, list[str]] = {1: [], 3: []}
        # Room dict now stores players AND the selected overs
        self.private_rooms: dict[str, dict] = {} 
        self.matches: dict[str, dict] = {} 

    async def connect(self, websocket: WebSocket, name: str, room: str = None, overs: int = 1):
        await websocket.accept()
        player_id = str(uuid.uuid4())
        self.active_connections[player_id] = {"ws": websocket, "name": name, "room": room, "overs": overs}

        if room:
            # FRIEND MATCH LOGIC
            if room not in self.private_rooms:
                self.private_rooms[room] = {"players": [], "overs": overs}
            
            if len(self.private_rooms[room]["players"]) >= 2:
                await websocket.send_json({"type": "error", "message": "Room is full"})
                await websocket.close()
                return None
            
            # The room dictates the match length (overrides joiner if different)
            actual_overs = self.private_rooms[room]["overs"]
            self.active_connections[player_id]["overs"] = actual_overs
            self.private_rooms[room]["players"].append(player_id)

            if len(self.private_rooms[room]["players"]) == 2:
                p1, p2 = self.private_rooms[room]["players"][0], self.private_rooms[room]["players"][1]
                await self.setup_match(p1, p2, actual_overs)
            else:
                await websocket.send_json({"type": "queue", "message": "Waiting for friend to join..."})
        else:
            # QUICK MATCH LOGIC
            if overs not in self.quick_match_queue:
                self.quick_match_queue[overs] = []
                
            self.quick_match_queue[overs].append(player_id)
            if len(self.quick_match_queue[overs]) >= 2:
                p1 = self.quick_match_queue[overs].pop(0)
                p2 = self.quick_match_queue[overs].pop(0)
                await self.setup_match(p1, p2, overs)
            else:
                await websocket.send_json({"type": "queue", "message": f"Searching for {overs}-Over opponent..."})
        
        return player_id

    async def setup_match(self, p1: str, p2: str, overs: int):
        toss_winner = random.choice(["p1", "p2"])
        
        self.matches[p1] = {"opp": p2, "role": "p1", "move": None}
        self.matches[p2] = {"opp": p1, "role": "p2", "move": None}

        await self.active_connections[p1]["ws"].send_json({
            "type": "match_found",
            "player_id": "p1",
            "opp_name": self.active_connections[p2]["name"],
            "toss_winner": "p1" if toss_winner == "p1" else "p2",
            "overs": overs
        })
        
        await self.active_connections[p2]["ws"].send_json({
            "type": "match_found",
            "player_id": "p2",
            "opp_name": self.active_connections[p1]["name"],
            "toss_winner": "p2" if toss_winner == "p2" else "p1",
            "overs": overs
        })

    def disconnect(self, player_id: str):
        if player_id not in self.active_connections:
            return None

        # Clean private rooms
        room = self.active_connections[player_id].get("room")
        if room and room in self.private_rooms:
            if player_id in self.private_rooms[room]["players"]:
                self.private_rooms[room]["players"].remove(player_id)
            if len(self.private_rooms[room]["players"]) == 0:
                del self.private_rooms[room]
        
        # Clean quick match queue
        overs = self.active_connections[player_id].get("overs", 1)
        if player_id in self.quick_match_queue.get(overs, []):
            self.quick_match_queue[overs].remove(player_id)

        # Notify opponent if mid-match
        opp_id = None
        if player_id in self.matches:
            opp_id = self.matches[player_id]["opp"]
            del self.matches[player_id]

        del self.active_connections[player_id]
        return opp_id

manager = ConnectionManager()

@app.websocket("/ws/pvp")
async def websocket_endpoint(websocket: WebSocket, name: str = "Guest", room: str = None, overs: int = 1):
    player_id = await manager.connect(websocket, name, room, overs)
    
    if not player_id:
        return

    try:
        while True:
            data_str = await websocket.receive_text()
            data = json.loads(data_str)
            
            if player_id not in manager.matches:
                continue
                
            match_data = manager.matches[player_id]
            opp_id = match_data["opp"]
            
            if opp_id not in manager.active_connections:
                continue

            if data["type"] == "toss_choice":
                await manager.active_connections[player_id]["ws"].send_json({
                    "type": "toss_result", "chooser": match_data["role"], "choice": data["choice"]
                })
                await manager.active_connections[opp_id]["ws"].send_json({
                    "type": "toss_result", "chooser": match_data["role"], "choice": data["choice"]
                })
            
            elif data["type"] == "move":
                match_data["move"] = data
                opp_match_data = manager.matches[opp_id]
                
                if match_data["move"] is not None and opp_match_data["move"] is not None:
                    p1_id = player_id if match_data["role"] == "p1" else opp_id
                    p2_id = opp_id if match_data["role"] == "p1" else player_id
                    
                    p1_move_data = manager.matches[p1_id]["move"]
                    p2_move_data = manager.matches[p2_id]["move"]
                    
                    result = {
                        "type": "turn_result",
                        "p1_move": p1_move_data,
                        "p2_move": p2_move_data
                    }
                    
                    manager.matches[p1_id]["move"] = None
                    manager.matches[p2_id]["move"] = None
                    
                    await manager.active_connections[p1_id]["ws"].send_json(result)
                    await manager.active_connections[p2_id]["ws"].send_json(result)

    except WebSocketDisconnect:
        opp_id = manager.disconnect(player_id)
        if opp_id and opp_id in manager.active_connections:
            try:
                await manager.active_connections[opp_id]["ws"].send_json({"type": "opponent_disconnected"})
            except:
                pass

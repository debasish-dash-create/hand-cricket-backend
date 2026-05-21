import json
import uuid
import random
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

class ConnectionManager:
    def __init__(self):
        self.waiting_socket = None
        self.waiting_name = None
        self.rooms = {}

    async def connect(self, websocket: WebSocket, name: str):
        await websocket.accept()
        
        if self.waiting_socket is None:
            self.waiting_socket = websocket
            self.waiting_name = name
            await websocket.send_json({"type": "queue", "message": "Waiting for an opponent..."})
            return None, None
            
        else:
            room_id = str(uuid.uuid4())
            p1 = self.waiting_socket
            p1_name = self.waiting_name
            p2 = websocket
            p2_name = name
            
            self.rooms[room_id] = {
                "p1": p1, 
                "p2": p2, 
                "p1_name": p1_name,
                "p2_name": p2_name,
                "p1_move": None, 
                "p2_move": None
            }
            
            self.waiting_socket = None
            self.waiting_name = None
            
            # Randomly pick toss winner
            toss_winner = random.choice(["p1", "p2"])
            
            await p1.send_json({
                "type": "match_found", 
                "player_id": "p1", 
                "opp_name": p2_name,
                "toss_winner": toss_winner
            })
            await p2.send_json({
                "type": "match_found", 
                "player_id": "p2", 
                "opp_name": p1_name,
                "toss_winner": toss_winner
            })
            
            return room_id, "p2"

    async def handle_message(self, room_id: str, player_id: str, data: dict):
        room = self.rooms.get(room_id)
        if not room:
            return
            
        msg_type = data.get("type")
        
        # Handle the coin toss selection
        if msg_type == "toss_choice":
            choice = data.get("choice")
            await room["p1"].send_json({"type": "toss_result", "chooser": player_id, "choice": choice})
            await room["p2"].send_json({"type": "toss_result", "chooser": player_id, "choice": choice})
            
        # Handle gameplay moves
        elif msg_type == "move":
            if player_id == "p1":
                room["p1_move"] = data
            else:
                room["p2_move"] = data
                
            if room["p1_move"] and room["p2_move"]:
                result = {
                    "type": "turn_result",
                    "p1_move": room["p1_move"],
                    "p2_move": room["p2_move"]
                }
                await room["p1"].send_json(result)
                await room["p2"].send_json(result)
                room["p1_move"] = None
                room["p2_move"] = None

    def disconnect(self, websocket: WebSocket, room_id: str):
        if self.waiting_socket == websocket:
            self.waiting_socket = None
            self.waiting_name = None
            
        if room_id and room_id in self.rooms:
            del self.rooms[room_id]

manager = ConnectionManager()

@app.websocket("/ws/pvp")
async def websocket_endpoint(websocket: WebSocket, name: str = "Guest"):
    room_id, player_id = await manager.connect(websocket, name)
    
    if room_id is None:
        player_id = "p1"
        
    active_room_id = room_id
    
    try:
        while True:
            data = await websocket.receive_text()
            msg_data = json.loads(data)
            
            # Dynamically find the active room if it wasn't assigned at connection
            if not active_room_id:
                for rid, room in manager.rooms.items():
                    if room["p1"] == websocket or room["p2"] == websocket:
                        active_room_id = rid
                        break
                    
            if active_room_id:
                await manager.handle_message(active_room_id, player_id, msg_data)
                
    except WebSocketDisconnect:
        manager.disconnect(websocket, active_room_id)

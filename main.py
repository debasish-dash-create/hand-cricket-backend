from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from motor.motor_asyncio import AsyncIOMotorClient
import uuid
import random
import json
import asyncio

# Replace YOUR_PASSWORD with the password you just created.
MONGO_URI = "mongodb+srv://dchoudhurydebasish_db_user:Dash_2003@cluster0.dh7k21g.mongodb.net/?appName=Cluster0"

# Global database variable
db = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db
    # Connect to the cluster
    client = AsyncIOMotorClient(MONGO_URI)
    # Target our specific game database (creates it automatically if it doesn't exist)
    db = client.neon_cricket_db 
    print("Connected to MongoDB!")
    
    # THE MAGIC: Create a TTL (Time-To-Live) index for the 72-hour auto-delete.
    # 72 hours = 259,200 seconds. MongoDB will silently clean up old matches in the background.
    await db.match_history.create_index("created_at", expireAfterSeconds=259200)
    
    yield
    
    # Clean up the connection when the server shuts down
    client.close()
    print("Disconnected from MongoDB.")

# Inject the lifespan manager into FastAPI
app = FastAPI(lifespan=lifespan)

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
        self.quick_match_queue: dict[int, list[str]] = {1: [], 3: []}
        
        # Room & Lobby State
        self.rooms: dict[str, dict] = {} 
        
        # Active Game States
        self.matches_1v1: dict[str, dict] = {} 
        self.matches_2v2: dict[str, dict] = {} 

    async def connect(self, websocket: WebSocket, name: str, room: str = None, mode: str = "1v1", overs: int = 1):
        await websocket.accept()
        player_id = str(uuid.uuid4())
        
        # THE FIX: Tell the frontend its ID immediately so it knows it is the host
        await websocket.send_json({"type": "connected", "player_id": player_id})
        
        self.active_connections[player_id] = {"ws": websocket, "name": name, "room": room}

        if room:
            # --- FRIEND LOBBY LOGIC ---
            if room not in self.rooms:
                self.rooms[room] = {
                    "mode": mode,
                    "host": player_id,
                    "overs": 4 if mode == "2v2" else overs,
                    "status": "lobby",
                    "team_1": {"name": "TEAM 1", "players": [None, None] if mode == "2v2" else [None]},
                    "team_2": {"name": "TEAM 2", "players": [None, None] if mode == "2v2" else [None]},
                    "captains": {"team_1": None, "team_2": None}
                }
            
            state = self.rooms[room]
            if state["status"] != "lobby":
                await websocket.send_json({"type": "error", "message": "Match has already started."})
                await websocket.close()
                return None

            assigned = False
            for team in ["team_1", "team_2"]:
                for i in range(len(state[team]["players"])):
                    if state[team]["players"][i] is None:
                        state[team]["players"][i] = {"id": player_id, "name": name, "is_cpu": False}
                        assigned = True
                        break
                if assigned: break
            
            if not assigned:
                await websocket.send_json({"type": "error", "message": "Room is full."})
                await websocket.close()
                return None

            await self.broadcast_lobby_state(room)
            
        else:
            # --- QUICK MATCH 1v1 LOGIC ---
            if overs not in self.quick_match_queue:
                self.quick_match_queue[overs] = []
            self.quick_match_queue[overs].append(player_id)
            if len(self.quick_match_queue[overs]) >= 2:
                p1 = self.quick_match_queue[overs].pop(0)
                p2 = self.quick_match_queue[overs].pop(0)
                await self.setup_1v1_match(p1, p2, overs)
            else:
                await websocket.send_json({"type": "queue", "message": f"Searching for {overs}-Over opponent..."})
        
        return player_id

    async def broadcast_lobby_state(self, room: str):
        if room not in self.rooms: return
        state = self.rooms[room]
        
        payload = {
            "type": "lobby_update",
            "host": state["host"],
            "team_1": state["team_1"],
            "team_2": state["team_2"],
            "mode": state["mode"]
        }
        
        for team in ["team_1", "team_2"]:
            for slot in state[team]["players"]:
                if slot and not slot["is_cpu"]:
                    pid = slot["id"]
                    if pid in self.active_connections:
                        await self.active_connections[pid]["ws"].send_json(payload)

    async def setup_1v1_match(self, p1: str, p2: str, overs: int):
        toss_winner = random.choice(["p1", "p2"])
        # Track the overs and initialize rematch as False
        self.matches_1v1[p1] = {"opp": p2, "role": "p1", "move": None, "overs": overs, "rematch": False}
        self.matches_1v1[p2] = {"opp": p1, "role": "p2", "move": None, "overs": overs, "rematch": False}
        
        await self.active_connections[p1]["ws"].send_json({
            "type": "match_found", "player_id": "p1", "opp_name": self.active_connections[p2]["name"],
            "toss_winner": "p1" if toss_winner == "p1" else "p2", "overs": overs
        })
        await self.active_connections[p2]["ws"].send_json({
            "type": "match_found", "player_id": "p2", "opp_name": self.active_connections[p1]["name"],
            "toss_winner": "p2" if toss_winner == "p2" else "p1", "overs": overs
        })

    def init_2v2_game_state(self, room: str, bat_team: str, bowl_team: str):
        state = self.rooms[room]
        # P1 = Striker, P2 = Non-Striker
        batters = [p for p in state[bat_team]["players"] if p is not None]
        # P3 = Active Bowler, P4 = Next Bowler
        bowlers = [p for p in state[bowl_team]["players"] if p is not None]
        
        self.matches_2v2[room] = {
            "overs": state["overs"],
            "balls": 0,
            "innings": 1,
            "batting_team": bat_team,
            "bowling_team": bowl_team,
            "score": {bat_team: 0, bowl_team: 0},
            "wickets": {bat_team: 0, bowl_team: 0},
            "target": None,
            "striker": batters[0],
            "non_striker": batters[1] if len(batters) > 1 else None,
            "bowler": bowlers[0],
            "next_bowler": bowlers[1] if len(bowlers) > 1 else bowlers[0],
            "moves": {}
        }
        self.auto_fill_cpu_moves(room)

    def auto_fill_cpu_moves(self, room: str):
        game = self.matches_2v2[room]
        striker = game["striker"]
        bowler = game["bowler"]
        
        if striker and striker["is_cpu"] and striker["id"] not in game["moves"]:
            game["moves"][striker["id"]] = random.randint(1, 6)
            
        if bowler and bowler["is_cpu"] and bowler["id"] not in game["moves"]:
            game["moves"][bowler["id"]] = random.randint(1, 6)

    async def broadcast_2v2_event(self, room: str, payload: dict):
        state = self.rooms[room]
        for team in ["team_1", "team_2"]:
            for slot in state[team]["players"]:
                if slot and not slot["is_cpu"]:
                    pid = slot["id"]
                    if pid in self.active_connections:
                        await self.active_connections[pid]["ws"].send_json(payload)

    def disconnect(self, player_id: str):
        if player_id not in self.active_connections: return None
        
        room = self.active_connections[player_id].get("room")
        if room and room in self.rooms:
            state = self.rooms[room]
            for team in ["team_1", "team_2"]:
                for i in range(len(state[team]["players"])):
                    slot = state[team]["players"][i]
                    if slot and slot["id"] == player_id:
                        state[team]["players"][i] = None
            
            is_empty = all(slot is None or slot["is_cpu"] for team in ["team_1", "team_2"] for slot in state[team]["players"])
            
            if is_empty:
                if room in self.rooms: del self.rooms[room]
                if room in self.matches_2v2: del self.matches_2v2[room]
            else:
                if state["host"] == player_id:
                    for team in ["team_1", "team_2"]:
                        for slot in state[team]["players"]:
                            if slot and not slot["is_cpu"]:
                                state["host"] = slot["id"]
                                break
                if state["status"] == "lobby":
                    asyncio.create_task(self.broadcast_lobby_state(room))

        for overs_queue in self.quick_match_queue.values():
            if player_id in overs_queue: overs_queue.remove(player_id)

        opp_id = None
        if player_id in self.matches_1v1:
            opp_id = self.matches_1v1[player_id]["opp"]
            del self.matches_1v1[player_id]

        del self.active_connections[player_id]
        return opp_id

manager = ConnectionManager()

@app.websocket("/ws/pvp")
async def websocket_endpoint(websocket: WebSocket, name: str = "Guest", room: str = None, mode: str = "1v1", overs: int = 1):
    player_id = await manager.connect(websocket, name, room, mode, overs)
    if not player_id: return

    try:
        while True:
            data_str = await websocket.receive_text()
            data = json.loads(data_str)
            msg_type = data.get("type")

            # --- LOBBY MANAGEMENT (HOST) ---
            if room and room in manager.rooms and manager.rooms[room]["status"] == "lobby":
                state = manager.rooms[room]
                
                if msg_type == "change_team_name":
                    team = data.get("team")
                    if team in ["team_1", "team_2"]:
                        state[team]["name"] = data.get("new_name")
                        await manager.broadcast_lobby_state(room)

                elif msg_type == "add_cpu" and player_id == state["host"]:
                    team = data.get("team")
                    slot_index = data.get("slot_index")
                    if state[team]["players"][slot_index] is None:
                        state[team]["players"][slot_index] = {"id": f"cpu_{uuid.uuid4().hex[:6]}", "name": "CPU", "is_cpu": True}
                        await manager.broadcast_lobby_state(room)

                elif msg_type == "start_match" and player_id == state["host"]:
                    all_full = all(slot is not None for team in ["team_1", "team_2"] for slot in state[team]["players"])
                    if not all_full:
                        await websocket.send_json({"type": "error", "message": "All slots must be filled (Players or CPU)."})
                        continue
                    
                    state["status"] = "toss"
                    state["captains"]["team_1"] = random.choice(state["team_1"]["players"])
                    state["captains"]["team_2"] = random.choice(state["team_2"]["players"])
                    toss_winner = random.choice(["team_1", "team_2"])
                    
                    payload = {
                        "type": "init_toss",
                        "team_1_captain": state["captains"]["team_1"],
                        "team_2_captain": state["captains"]["team_2"],
                        "toss_winner_team": toss_winner
                    }
                    await manager.broadcast_2v2_event(room, payload)

                    # Auto-Toss Logic for CPU Captain
                    winner_captain = state["captains"][toss_winner]
                    if winner_captain["is_cpu"]:
                        await asyncio.sleep(2)
                        choice = random.choice(["bat", "bowl"])
                        bat_team = toss_winner if choice == "bat" else ("team_1" if toss_winner == "team_2" else "team_2")
                        bowl_team = "team_1" if bat_team == "team_2" else "team_2"
                        
                        manager.init_2v2_game_state(room, bat_team, bowl_team)
                        await manager.broadcast_2v2_event(room, {"type": "toss_result", "choice": choice, "batting_team": bat_team})

            # --- 2V2 TOSS CHOICE ---
            elif room and room in manager.rooms and manager.rooms[room]["status"] == "toss":
                state = manager.rooms[room]
                if msg_type == "toss_choice":
                    bat_team = data.get("team") if data.get("choice") == "bat" else ("team_1" if data.get("team") == "team_2" else "team_2")
                    bowl_team = "team_1" if bat_team == "team_2" else "team_2"
                    
                    manager.init_2v2_game_state(room, bat_team, bowl_team)
                    await manager.broadcast_2v2_event(room, {"type": "toss_result", "choice": data["choice"], "batting_team": bat_team})

            # --- 2V2 GAMEPLAY LOOP ---
            elif room and room in manager.matches_2v2:
                game = manager.matches_2v2[room]
                if msg_type == "move":
                    game["moves"][player_id] = data["move"]
                    manager.auto_fill_cpu_moves(room)
                    
                    striker = game["striker"]
                    bowler = game["bowler"]
                    
                    # If both required moves are logged, resolve the ball
                    if striker["id"] in game["moves"] and bowler["id"] in game["moves"]:
                        s_move = game["moves"][striker["id"]]
                        b_move = game["moves"][bowler["id"]]
                        
                        is_out = (s_move == b_move)
                        runs = 0
                        
                        if is_out:
                            game["wickets"][game["batting_team"]] += 1
                            if game["wickets"][game["batting_team"]] == 1:
                                game["striker"] = game["non_striker"]
                                game["non_striker"] = None
                        else:
                            runs = s_move
                            game["score"][game["batting_team"]] += runs
                            
                        game["balls"] += 1
                        end_of_over = (game["balls"] % 6 == 0)
                        
                        # Strike Rotation Logic (1, 3, 5 runs OR End of Over)
                        if runs in [1, 3, 5] and game["non_striker"] is not None:
                            game["striker"], game["non_striker"] = game["non_striker"], game["striker"]
                            
                        if end_of_over and game["non_striker"] is not None:
                            game["striker"], game["non_striker"] = game["non_striker"], game["striker"]
                            
                        if end_of_over:
                            game["bowler"], game["next_bowler"] = game["next_bowler"], game["bowler"]

                        # Check Innings End
                        max_balls = game["overs"] * 6
                        innings_over = False
                        
                        if game["balls"] >= max_balls or game["wickets"][game["batting_team"]] == 2:
                            innings_over = True
                        if game["innings"] == 2 and game["score"][game["batting_team"]] > game["target"]:
                            innings_over = True
                            
                        if innings_over:
                            if game["innings"] == 1:
                                game["innings"] = 2
                                game["target"] = game["score"][game["batting_team"]]
                                game["batting_team"], game["bowling_team"] = game["bowling_team"], game["batting_team"]
                                game["balls"] = 0
                                
                                state = manager.rooms[room]
                                batters = [p for p in state[game["batting_team"]]["players"] if p is not None]
                                bowlers = [p for p in state[game["bowling_team"]]["players"] if p is not None]
                                game["striker"], game["non_striker"] = batters[0], (batters[1] if len(batters) > 1 else None)
                                game["bowler"], game["next_bowler"] = bowlers[0], (bowlers[1] if len(bowlers) > 1 else bowlers[0])
                            else:
                                pass # Match ends, handled by frontend checking score vs target

                        payload = {
                            "type": "turn_result_2v2",
                            "striker_move": s_move,
                            "bowler_move": b_move,
                            "runs": runs,
                            "is_out": is_out,
                            "innings_over": innings_over,
                            "new_state": game
                        }
                        
                        game["moves"].clear()
                        await manager.broadcast_2v2_event(room, payload)
                        manager.auto_fill_cpu_moves(room)

            # --- 1V1 QUICK MATCH GAMEPLAY LOOP ---
            elif player_id in manager.matches_1v1:
                match_data = manager.matches_1v1[player_id]
                opp_id = match_data["opp"]
                if opp_id not in manager.active_connections: continue

                if msg_type == "toss_choice":
                    await manager.active_connections[player_id]["ws"].send_json({"type": "toss_result", "chooser": match_data["role"], "choice": data["choice"]})
                    await manager.active_connections[opp_id]["ws"].send_json({"type": "toss_result", "chooser": match_data["role"], "choice": data["choice"]})
                
                elif msg_type == "move":
                    match_data["move"] = data
                    opp_match_data = manager.matches_1v1[opp_id]
                    if match_data["move"] is not None and opp_match_data["move"] is not None:
                        p1_id = player_id if match_data["role"] == "p1" else opp_id
                        p2_id = opp_id if match_data["role"] == "p1" else player_id
                        
                        result = {
                            "type": "turn_result",
                            "p1_move": manager.matches_1v1[p1_id]["move"],
                            "p2_move": manager.matches_1v1[p2_id]["move"]
                        }
                        manager.matches_1v1[p1_id]["move"] = None
                        manager.matches_1v1[p2_id]["move"] = None
                        
                        await manager.active_connections[p1_id]["ws"].send_json(result)
                        await manager.active_connections[p2_id]["ws"].send_json(result)

                # --- NEW REMATCH LOGIC GOES HERE ---
                elif msg_type == "rematch_request":
                    match_data = manager.matches_1v1.get(player_id)
                    if match_data:
                        opp_id = match_data["opp"]
                        match_data["rematch"] = True
                        
                        opp_match_data = manager.matches_1v1.get(opp_id)
                        if opp_match_data and opp_match_data.get("rematch"):
                            # Both players clicked Rematch! Reset state and trigger match_found again
                            match_data["rematch"] = False
                            opp_match_data["rematch"] = False
                            
                            overs = match_data.get("overs", 1)
                            new_toss_winner = random.choice(["p1", "p2"])
                            
                            await websocket.send_json({
                                "type": "rematch_accepted"
                            })
                            await manager.active_connections[opp_id]["ws"].send_json({
                                "type": "rematch_accepted"
                            })
                            
                            # Send them new match configurations
                            await websocket.send_json({
                                "type": "match_found", "player_id": match_data["role"], 
                                "opp_name": manager.active_connections[opp_id]["name"],
                                "toss_winner": match_data["role"] if new_toss_winner == match_data["role"] else opp_match_data["role"], 
                                "overs": overs
                            })
                            await manager.active_connections[opp_id]["ws"].send_json({
                                "type": "match_found", "player_id": opp_match_data["role"], 
                                "opp_name": manager.active_connections[player_id]["name"],
                                "toss_winner": opp_match_data["role"] if new_toss_winner == opp_match_data["role"] else match_data["role"], 
                                "overs": overs
                            })
    except WebSocketDisconnect:
        opp_id = manager.disconnect(player_id)
        if opp_id and opp_id in manager.active_connections:
            try: await manager.active_connections[opp_id]["ws"].send_json({"type": "opponent_disconnected"})
            except: pass

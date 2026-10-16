import json
import os
import sys
import random
import signal
import time
import threading
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple
from flask import Flask

if os.name == 'nt':
    os.system('color')

try:
    import websocket
except ImportError:
    os.system(f"{sys.executable} -m pip install websocket-client")
    import websocket

try:
    import requests
except ImportError:
    os.system(f"{sys.executable} -m pip install requests")
    import requests

GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
API_BASE = "https://discord.com/api/v10"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
DISCORD_EPOCH = 1420070400000

GAME_LIST = ["League of Legends", "Valorant", "Genshin Impact", "Minecraft", "Grand Theft Auto V", "Roblox", "Counter-Strike 2", "Dota 2", "Apex Legends", "Visual Studio Code"]

class Log:
    RESET = '\033[0m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    GRAY = '\033[90m'
    BOLD = '\033[1m'
    MAGENTA = '\033[95m'
    WHITE = '\033[97m'
    BLUE = '\033[94m'

    @staticmethod
    def _time():
        return f"{Log.GRAY}[{datetime.now().strftime('%H:%M:%S')}]{Log.RESET}"

    @staticmethod
    def info(msg):
        print(f"{Log._time()} {Log.CYAN}{Log.BOLD}[INFO]{Log.RESET} {msg}")

    @staticmethod
    def success(msg):
        print(f"{Log._time()} {Log.GREEN}{Log.BOLD}[SUCCESS]{Log.RESET} {msg}")

    @staticmethod
    def warn(msg):
        print(f"{Log._time()} {Log.YELLOW}{Log.BOLD}[WARN]{Log.RESET} {msg}")

    @staticmethod
    def error(msg):
        print(f"{Log._time()} {Log.RED}{Log.BOLD}[ERROR]{Log.RESET} {msg}")
        
    @staticmethod
    def draw_banner():
        try:
            term_width = shutil.get_terminal_size((90, 20)).columns
        except Exception:
            term_width = 90

        colors = [Log.RED, Log.YELLOW, Log.GREEN, Log.CYAN, Log.BLUE, Log.MAGENTA, Log.WHITE]
        
        ascii_art = [
            "███████╗██╗   ██╗██████╗  ██████╗ ██╗  ██╗    ████████╗ ██████╗ ██╗  ██╗███████╗███╗   ██╗",
            "╚══███╔╝╚██╗ ██╔╝██╔══██╗██╔═══██╗╚██╗██╔╝    ╚══██╔══╝██╔═══██╗██║ ██╔╝██╔════╝████╗  ██║",
            "  ███╔╝  ╚████╔╝ ██████╔╝██║   ██║ ╚███╔╝        ██║   ██║   ██║█████╔╝ █████╗  ██╔██╗ ██║",
            " ███╔╝    ╚██╔╝  ██╔══██╗██║   ██║ ██╔██╗        ██║   ██║   ██║██╔═██╗ ██╔══╝  ██║╚██╗██║",
            "███████╗   ██║   ██║  ██║╚██████╔╝██╔╝ ██╗       ██║   ╚██████╔╝██║  ██╗███████╗██║ ╚████║",
            "╚══════╝   ╚═╝   ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═╝       ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝╚═╝  ╚═══╝"
        ]
        
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\n")
        
        print(f"{Log.BOLD}{Log.CYAN}{'==================================================='.center(term_width)}{Log.RESET}")
        print(f"{Log.BOLD}{Log.YELLOW}{'ZYROX TOKEN'.center(term_width)}{Log.RESET}")
        print(f"{Log.BOLD}{Log.CYAN}{'==================================================='.center(term_width)}{Log.RESET}")
        print("\n")
        
        for i, line in enumerate(ascii_art):
            color = colors[i % len(colors)]
            print(f"{Log.BOLD}{color}{line.center(term_width)}{Log.RESET}")
            
        print("\n")
        print(f"{Log.BOLD}{Log.WHITE}{'Tool By: @awrt_205'.center(term_width)}{Log.RESET}")
        print(f"{Log.BOLD}{Log.MAGENTA}{'Link Server Discord: https://discord.com/invite/zyroxcity'.center(term_width)}{Log.RESET}")
        print("\n")

def get_account_age(user_id: str) -> str:
    try:
        timestamp_ms = (int(user_id) >> 22) + DISCORD_EPOCH
        creation_date = datetime.fromtimestamp(timestamp_ms / 1000.0)
        delta = datetime.now() - creation_date
        return f"{creation_date.strftime('%Y-%m-%d')} {delta.days} DAYS"
    except Exception:
        return "UNKNOWN"

def get_public_ip() -> str:
    try:
        return requests.get("https://api.ipify.org", timeout=5).text
    except:
        return "UNKNOWN"

class DiscordTokenKeeper:
    def __init__(self, token: str, index: int, game_name: str, account_info: str = ""):
        self.token = token.strip()
        self.index = index
        self.game_name = game_name
        self.account_info = account_info
        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_thread: Optional[threading.Thread] = None

        self.sequence: Optional[int] = None
        self.session_id: Optional[str] = None
        self.resume_gateway_url: Optional[str] = None

        self.heartbeat_interval: float = 41250
        self.last_heartbeat_ack: float = time.time()

        self.reconnect_attempts: int = 0
        self.max_reconnect_delay: float = 60.0
        self.base_reconnect_delay: float = 1.0
        self.should_reconnect: bool = True

        self.is_ready: bool = False
        self.is_running: bool = False
        self.user_info: Dict[str, Any] = {}

    def get_reconnect_delay(self) -> float:
        delay = min(
            self.base_reconnect_delay * (1.5 ** self.reconnect_attempts),
            self.max_reconnect_delay
        )
        jitter = delay * 0.25 * (random.random() * 2 - 1)
        return delay + jitter

    def send_heartbeat(self) -> None:
        if self.ws and self.ws.sock and self.ws.sock.connected:
            try:
                self.ws.send(json.dumps({"op": 1, "d": self.sequence}))
            except:
                pass

    def start_heartbeat_loop(self, interval_ms: float) -> None:
        self.heartbeat_interval = interval_ms / 1000.0
        def heartbeat_loop():
            time.sleep(random.random() * self.heartbeat_interval)
            while self.is_running and self.should_reconnect:
                if self.ws and self.ws.sock and self.ws.sock.connected:
                    self.send_heartbeat()
                else:
                    break
                time.sleep(self.heartbeat_interval)
        threading.Thread(target=heartbeat_loop, daemon=True).start()

    def send_identify(self) -> None:
        identify_payload = {
            "op": 2,
            "d": {
                "token": self.token,
                "capabilities": 16381,
                "properties": {
                    "os": "Windows",
                    "browser": "Discord Client",
                    "device": "desktop",
                },
                "presence": {
                    "status": "online",
                    "since": 0,
                    "activities": [{"name": self.game_name, "type": 0}],
                    "afk": False
                },
                "compress": False,
                "intents": 0
            }
        }
        self.ws.send(json.dumps(identify_payload))

    def send_resume(self) -> None:
        resume_payload = {
            "op": 6,
            "d": {
                "token": self.token,
                "session_id": self.session_id,
                "seq": self.sequence
            }
        }
        self.ws.send(json.dumps(resume_payload))

    def on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        try:
            payload = json.loads(message)
            op = payload.get("op")
            data = payload.get("d")
            seq = payload.get("s")
            event_type = payload.get("t")

            if seq is not None:
                self.sequence = seq

            if op == 10:
                self.start_heartbeat_loop(data.get("heartbeat_interval", 41250))
                if self.session_id and self.sequence is not None:
                    self.send_resume()
                else:
                    self.send_identify()

            elif op == 11:
                self.last_heartbeat_ack = time.time()

            elif op == 0:
                self.handle_dispatch(event_type, data)

            elif op == 7:
                self.reconnect()

            elif op == 9:
                can_resume = data if isinstance(data, bool) else False
                if not can_resume:
                    self.session_id = None
                    self.sequence = None
                time.sleep(random.uniform(1, 3))
                self.reconnect()
        except:
            pass

    def handle_dispatch(self, event_type: str, data: dict) -> None:
        if event_type == "READY":
            self.session_id = data.get("session_id")
            self.resume_gateway_url = data.get("resume_gateway_url")
            self.is_ready = True
            self.is_running = True
            self.reconnect_attempts = 0
            self.user_info = data.get("user", {})
            Log.success(f"{Log.WHITE}TOKEN #{self.index:02d} ĐÃ ONLINE | CHƠI: {Log.YELLOW}{self.game_name}{Log.RESET}")

        elif event_type == "RESUMED":
            self.reconnect_attempts = 0
            self.is_ready = True
            self.is_running = True
            Log.success(f"{Log.WHITE}TOKEN #{self.index:02d} KHÔI PHỤC KẾT NỐI THÀNH CÔNG{Log.RESET}")

    def on_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        pass

    def on_close(self, ws: websocket.WebSocketApp, close_status_code: int, close_msg: str) -> None:
        self.is_running = False
        self.is_ready = False
        if self.should_reconnect:
            delay = self.get_reconnect_delay()
            self.reconnect_attempts += 1
            Log.error(f"TOKEN #{self.index:02d} BỊ NGẮT KẾT NỐI - ĐANG THỬ LẠI SAU {delay:.1f}S")
            threading.Timer(delay, self.start_connection).start()

    def on_open(self, ws: websocket.WebSocketApp) -> None:
        self.is_running = True

    def start_connection(self) -> None:
        if not self.should_reconnect:
            return
            
        try:
            gateway = self.resume_gateway_url or GATEWAY_URL
            self.ws = websocket.WebSocketApp(
                gateway,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                header={
                    "User-Agent": USER_AGENT,
                    "Origin": "https://discord.com"
                }
            )
            self.ws_thread = threading.Thread(
                target=self.ws.run_forever,
                kwargs={"ping_interval": 30, "ping_timeout": 10, "reconnect": 0},
                daemon=True
            )
            self.ws_thread.start()
        except Exception:
            if self.should_reconnect:
                threading.Timer(self.get_reconnect_delay(), self.start_connection).start()

    def reconnect(self) -> None:
        self.is_running = False
        if self.ws:
            try:
                self.ws.close()
            except:
                pass
        threading.Timer(random.uniform(0.5, 2.0), self.start_connection).start()

    def disconnect(self) -> None:
        self.should_reconnect = False
        self.is_running = False
        if self.ws:
            try:
                self.ws.close()
            except:
                pass

def validate_single_token(token: str) -> Tuple[bool, Optional[Dict], int]:
    try:
        headers = {
            "Authorization": token,
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json"
        }
        resp = requests.get(f"{API_BASE}/users/@me", headers=headers, timeout=10)
        
        if resp.status_code == 200:
            user_data = resp.json()
            guilds_count = 0
            try:
                guilds_resp = requests.get(f"{API_BASE}/users/@me/guilds", headers=headers, timeout=5)
                if guilds_resp.status_code == 200:
                    guilds_count = len(guilds_resp.json())
            except:
                pass
                
            return True, user_data, guilds_count
            
        elif resp.status_code == 429:
            return True, None, 0
            
        return False, None, 0
    except:
        return False, None, 0

class TokenManager:
    def __init__(self):
        self.keepers: List[DiscordTokenKeeper] = []
        self.monitor_thread: Optional[threading.Thread] = None
        self.is_running: bool = False

    def load_tokens(self) -> List[str]:
        # Ưu tiên tìm file trong thư mục Secret của Render
        secret_path = Path("/etc/secrets/tokens.txt")
        local_path = Path(__file__).parent / "tokens.txt"
        
        token_file = secret_path if secret_path.exists() else local_path

        if not token_file.exists():
            try:
                local_path.write_text("", encoding="utf-8")
            except:
                pass
            Log.warn("KHÔNG TÌM THẤY TOKENS.TXT - VUI LÒNG THÊM TOKEN VÀO SECRET FILE HOẶC THƯ MỤC GỐC")
            return []

        tokens = []
        for line in token_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and len(line) > 50:
                tokens.append(line)
        return tokens

    def save_tokens(self, tokens: List[str]) -> None:
        try:
            token_file = Path(__file__).parent / "tokens.txt"
            token_file.write_text("\n".join(tokens), encoding="utf-8")
        except Exception as e:
            Log.warn(f"KHÔNG THỂ LƯU TOKENS.TXT (Có thể do dùng Secret File): {e}")

    def process_and_run(self, raw_tokens: List[str]) -> None:
        Log.info(f"BẮT ĐẦU KIỂM TRA VÀ ONLINE TUẦN TỰ CHO {len(raw_tokens)} TOKENS...")
        live_tokens = []
        
        for i, token in enumerate(raw_tokens):
            token_preview = token[:15] + "..." if len(token) > 15 else token
            is_live, user_data, guilds_count = validate_single_token(token)
            
            if is_live:
                username = "Unknown"
                user_id = "Unknown"
                email = "UNLINKED"
                phone = "UNLINKED"
                age_str = "Unknown Age"
                mfa_status = False
                
                if user_data:
                    username = user_data.get("username", "Unknown")
                    user_id = user_data.get("id", "Unknown")
                    email = user_data.get("email") or "UNLINKED"
                    phone = user_data.get("phone") or "UNLINKED"
                    mfa_status = user_data.get("mfa_enabled", False)
                    if user_id != "Unknown":
                        age_str = get_account_age(user_id)
                
                mfa_color = Log.GREEN if mfa_status else Log.RED
                mfa_text = "BẬT" if mfa_status else "TẮT"
                
                account_info = (
                    f"{Log.CYAN}USER: {username}{Log.RESET} | "
                    f"{Log.GRAY}ID: {user_id}{Log.RESET} | "
                    f"{Log.YELLOW}EMAIL: {email}{Log.RESET} | "
                    f"{Log.MAGENTA}SĐT: {phone}{Log.RESET} | "
                    f"{Log.WHITE}2FA: {mfa_color}{mfa_text}{Log.RESET} | "
                    f"{Log.GREEN}SERVER: {guilds_count}{Log.RESET} | "
                    f"{Log.BLUE}C.DATE: {age_str}{Log.RESET}"
                )
                
                Log.success(f"[{i+1}/{len(raw_tokens)}] {account_info}")
                live_tokens.append(token)
                
                random_game = random.choice(GAME_LIST)
                keeper = DiscordTokenKeeper(token, len(live_tokens), random_game, account_info)
                self.keepers.append(keeper)
                
                threading.Thread(target=keeper.start_connection, daemon=True).start()
                
                wait_time = 0
                timeout = 15
                while not keeper.is_ready and wait_time < timeout:
                    time.sleep(0.5)
                    wait_time += 0.5
                    
            else:
                Log.error(f"[{i+1}/{len(raw_tokens)}] DEAD HOẶC LỖI - {token_preview}")
                
        self.save_tokens(live_tokens)
        Log.info(f"HOÀN TẤT! ĐANG DUY TRÌ ONLINE CHO {Log.GREEN}{len(live_tokens)}{Log.RESET} TOKENS.")
        
        if not live_tokens:
            Log.error("TẤT CẢ TOKEN ĐỀU DEAD - KHÔNG CÓ TOKEN NÀO ĐỂ CHẠY")

    def start(self) -> None:
        raw_tokens = self.load_tokens()

        if not raw_tokens:
            Log.error("KHÔNG CÓ TOKEN TRONG TOKENS.TXT")
            return # Không sys.exit để web server vẫn chạy
            
        Log.draw_banner()
        
        ip_addr = get_public_ip()
        Log.info(f"ĐANG VẬN HÀNH TRÊN PUBLIC IP: {Log.YELLOW}{ip_addr}{Log.RESET}")
        
        self.is_running = True
        self.process_and_run(raw_tokens)
        self.start_monitor()

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.shutdown()

    def start_monitor(self) -> None:
        def monitor_loop():
            while self.is_running:
                time.sleep(60) 
                for keeper in self.keepers:
                    if keeper.should_reconnect and not keeper.is_running:
                        Log.warn(f"WATCHDOG: TOKEN #{keeper.index:02d} BỊ RỚT, ĐANG ÉP KẾT NỐI LẠI...")
                        keeper.reconnect()

        self.monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        self.monitor_thread.start()

    def signal_handler(self, signum, frame) -> None:
        print()
        Log.warn("NHẬN LỆNH DỪNG - ĐANG NGẮT KẾT NỐI TẤT CẢ TOKENS...")
        self.shutdown()

    def shutdown(self) -> None:
        self.is_running = False
        for keeper in self.keepers:
            keeper.disconnect()
        Log.success("TẤT CẢ TOKENS ĐÃ ĐƯỢC NGẮT KẾT NỐI AN TOÀN")

def validate_all_tokens() -> None:
    Log.draw_banner()
    
    token_file = Path(__file__).parent / "tokens.txt"
    if not token_file.exists():
        Log.error("KHÔNG TÌM THẤY TOKENS.TXT")
        return

    tokens = []
    for line in token_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and len(line) > 50:
            tokens.append(line)

    if not tokens:
        Log.error("KHÔNG CÓ TOKEN ĐỂ KIỂM TRA")
        return
        
    ip_addr = get_public_ip()
    Log.info(f"PUBLIC IP: {Log.YELLOW}{ip_addr}{Log.RESET}")
    Log.info(f"ĐANG KIỂM TRA {len(tokens)} TOKENS...\n")
    valid_count = 0

    for i, token in enumerate(tokens):
        token_preview = token[:15] + "..." if len(token) > 15 else token
        is_live, user_data, guilds_count = validate_single_token(token)
        
        if is_live:
            username = "Unknown"
            user_id = "Unknown"
            email = "UNLINKED"
            phone = "UNLINKED"
            age_str = "Unknown Age"
            mfa_status = False
            
            if user_data:
                username = user_data.get("username", "Unknown")
                user_id = user_data.get("id", "Unknown")
                email = user_data.get("email") or "UNLINKED"
                phone = user_data.get("phone") or "UNLINKED"
                mfa_status = user_data.get("mfa_enabled", False)
                if user_id != "Unknown":
                    age_str = get_account_age(user_id)
            
            mfa_color = Log.GREEN if mfa_status else Log.RED
            mfa_text = "BẬT" if mfa_status else "TẮT"
            
            account_info = (
                f"{Log.CYAN}USER: {username}{Log.RESET} | "
                f"{Log.GRAY}ID: {user_id}{Log.RESET} | "
                f"{Log.YELLOW}EMAIL: {email}{Log.RESET} | "
                f"{Log.MAGENTA}SĐT: {phone}{Log.RESET} | "
                f"{Log.WHITE}2FA: {mfa_color}{mfa_text}{Log.RESET} | "
                f"{Log.GREEN}SERVER: {guilds_count}{Log.RESET} | "
                f"{Log.BLUE}C.DATE: {age_str}{Log.RESET}"
            )
            
            Log.success(f"[{i+1:02d}] {account_info}")
            valid_count += 1
        else:
            Log.error(f"[{i+1:02d}] DEAD - {token_preview}")

    print(f"\n=> KẾT QUẢ: \033[92m{valid_count}\033[0m/\033[93m{len(tokens)}\033[0m TOKENS CÒN SỐNG.\n")

# ==========================================
# PHẦN WEB SERVER (FLASK) DÀNH CHO RENDER
# ==========================================
app = Flask(__name__)
manager = TokenManager()

@app.route('/')
def home():
    total_tokens = len(manager.keepers)
    online_tokens = sum(1 for k in manager.keepers if k.is_ready)
    
    html = f"""
    <!DOCTYPE html>
    <html>
        <head>
            <title>ZYROX TOKEN KEEPER</title>
            <meta http-equiv="refresh" content="30">
            <style>
                body {{ background-color: #121212; color: #ffffff; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; text-align: center; padding-top: 50px; margin: 0; }}
                .container {{ max-width: 800px; margin: auto; padding: 20px; }}
                h1 {{ color: #00ffcc; text-shadow: 0 0 10px #00ffcc; }}
                .card {{ background-color: #1e1e1e; border-radius: 10px; padding: 30px; margin-top: 30px; box-shadow: 0 4px 15px rgba(0,0,0,0.5); border: 1px solid #333; }}
                .status-text {{ font-size: 20px; margin-bottom: 20px; }}
                .online {{ color: #4CAF50; font-weight: bold; font-size: 28px; }}
                .offline {{ color: #F44336; font-weight: bold; font-size: 28px; }}
                .footer {{ margin-top: 40px; font-size: 14px; color: #888; }}
                .badge {{ background-color: #00ffcc; color: #000; padding: 5px 10px; border-radius: 5px; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>ZYROX TOKEN KEEPER</h1>
                <div class="card">
                    <div class="status-text">Trạng thái hệ thống: <span class="online">ĐANG HOẠT ĐỘNG</span></div>
                    <hr style="border-color: #333;">
                    <p style="font-size: 22px; margin-top: 20px;">Số lượng Token Online:</p>
                    <p><span class="online">{online_tokens}</span> <span style="color: #888;">/ {total_tokens}</span></p>
                    <p style="margin-top: 30px;"><span class="badge">Auto Refresh: 30s</span></p>
                </div>
                <div class="footer">
                    Tool By: @awrt_205 | Discord: <a href="https://discord.com/invite/zyroxcity" style="color: #00ffcc; text-decoration: none;">ZYROX CITY</a>
                </div>
            </div>
        </body>
    </html>
    """
    return html

if __name__ == "__main__":
    # 1. Chạy tool Discord ở background thread
    bot_thread = threading.Thread(target=manager.start, daemon=True)
    bot_thread.start()
    
    # 2. Chạy Web Server cho Render
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

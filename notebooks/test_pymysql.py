import pymysql
HOST = "127.0.0.1"
PORT = 3306
USER = "nirmitbansal"
PW = "nirmit@2708"   # replace

print("Trying pymysql connect to", HOST, "port", PORT)
try:
    conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PW, connect_timeout=5)
    print("Connected. Server version:", conn.get_server_info())
    conn.close()
except Exception as e:
    print("Connection failed:", repr(e))

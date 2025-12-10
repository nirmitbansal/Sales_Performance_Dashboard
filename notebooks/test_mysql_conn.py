import pymysql

HOST = "localhost"       # must be just host
PORT = 3306              # fix to the port you found (int)
USER = "nirmitbansal"
PW   = "nirmit@2708"

print("Testing raw pymysql connect to", HOST, "port", PORT)
try:
    conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PW, connect_timeout=5)
    print("Connected! Server version:", conn.get_server_info())
    conn.close()
except Exception as e:
    print("Connection failed:", repr(e))

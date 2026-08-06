import time
from truedata_ws.websocket.TD import TD

td_user = "Trial123"
td_pass = "mohd123"
td_port = 8086

print("Connecting to TrueData...")
td_obj = TD(td_user, td_pass, live_port=td_port)

@td_obj.trade_callback
def on_trade(msg):
    print("Received TICK:", msg.symbol, msg.ltp)

print("Starting live data for NIFTY26082524600CE and NIFTY 50...")
td_obj.start_live_data(["NIFTY26082524600CE", "NIFTY 50"])

time.sleep(10)
print("Done.")

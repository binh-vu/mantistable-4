from tornado.ioloop import IOLoop
from tornado.options import define, parse_command_line, options
from tornado.web import Application, RequestHandler
from tornado.websocket import WebSocketHandler, WebSocketClosedError
from tornado.httpserver import HTTPServer

from collections import defaultdict
from urllib.parse import urlparse
import logging
import signal
import time
import json


#TODO: Update this
define('debug', default=False, type=bool, help='Run in debug mode')
define('port', default=5001, type=int, help='Server websocket port')
define('allowed_hosts', default="localhost:5001", multiple=True,
    help='Allowed hosts for cross domain connections')


ws_app = None
class ClientHandler(WebSocketHandler):
    def __init__(self, *args):
        super().__init__(*args)
        self.channel = None

    # TODO: Check this
    def check_origin(self, origin):
        return True
        """
        allowed = super().check_origin(origin)
        parsed = urlparse(origin.lower())
        matched = any(parsed.netloc == host for host in options.allowed_hosts)
        
        print(allowed, parsed, matched)
        return options.debug or allowed or matched
        """
    
    def open(self, channel):
        self.application.add_subscriber(self, channel)
        self.channel = channel

        
    def on_message(self, message):
        pass
        #print("Received", message)
        #self.application.broadcast(self.channel, message)

        
    def on_close(self):
        self.application.remove_subscriber(self)

        
class FrontSyncApplication(Application):
    def __init__(self, **kwargs):
        routes = [
            (r'/(?P<channel>[a-z0-9]+)', ClientHandler),
        ]
        super().__init__(routes, **kwargs)
        self.subscriptions = {}
        
    def broadcast(self, channel, message):
        print("broadcast this!")
        peers = self.get_subscribers(channel)
        
        for peer in peers:
            try:
                peer.write_message(message)
            except WebSocketClosedError:
                # Remove dead peer
                self.remove_subscriber(peer)

    
    def add_subscriber(self, subscriber, channel):
        self.subscriptions[subscriber] = channel
    
    def remove_subscriber(self, subscriber):
        if subscriber in self.subscriptions:
            self.subscriptions.pop(subscriber)
        
    def get_subscribers(self, channel):
        return [
            sub
            for sub in self.subscriptions
            if self.subscriptions[sub] == channel
        ]


class CommandHandler(RequestHandler):     
    def post(self):
        raw = self.request.body.decode("utf-8")
        if len(raw) == 0:
            return self.write({
                'status': 'bad request',
                'request': raw
            })
            
        data = json.loads(raw)

        if "channel" in data and "payload" in data:
            channel = data["channel"]
            payload = data["payload"]
        
            ws_app.broadcast(channel, json.dumps(payload))
                
            self.write({
                'status': 'ok',
                'request': data
            })
        else:
            self.write({
                'status': 'bad request',
                'request': data
            })
                
                
        
        
def shutdown(server):
    ioloop = IOLoop.instance()
    logging.info('Stopping server.')
    server.stop()
    
    def finalize():
        ioloop.stop()
        logging.info('Stopped.')
    
    ioloop.add_timeout(time.time() + 1.5, finalize)

if __name__ == "__main__":
    parse_command_line()
    
    ws_app = FrontSyncApplication(debug=options.debug)
    ws_server = HTTPServer(ws_app)
    ws_server.listen(options.port)
    
    app = Application([("/", CommandHandler)], debug=True)
    app.listen(5000)
    
    #signal.signal(signal.SIGINT, lambda sig, frame: shutdown(ws_server))
    
    logging.info('Starting rest server on localhost:{}'.format(5000))
    logging.info('Starting ws server on localhost:{}'.format(options.port))
    IOLoop.instance().start()
 

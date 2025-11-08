from aiokafka import AIOKafkaProducer

class VideoProducer:
    def __init__(self, broker:str, topic:str, service_name:str):
        self.broker = broker
        self.topic = topic
        self._connected= False
        self.producer = AIOKafkaProducer( 
            bootstrap_servers=self.broker,
            value_serializer=lambda v: v, # .encode("utf-8"), use this for string data
            acks= 0,
            compression_type="lz4", #oder zstd ? beide sind schnell GPT sagt testen was besser ist
            max_batch_size=1024*16, #die anzahl an bytes die angesammelt werden bis eine Batch an Messages zu Kfaka gesendet wird (sollte bei uns 1 frame sein default ist 16kb ???? viel zu klein)
            linger_ms= 0, # Wartezeit befor ein Batch abgesendet wird auch wenn er nicht voll ist ... testen was gut ist 
            retry_backoff_ms=0, #verlust sollte uns egal sein denk ich.. option ist nut wichtig wenn acks > 0
            client_id=service_name, #selbsterklärend
            #request_timeout_ms=0,       # ebenso egal da acks = 0
            #reconnect_backoff_ms=10,        # wie schnell nach einem Connection error sollte ein reconnect verscuht werden 0 ist besser aber wenn er offline bleibt ist cpu Usage crazy ERROR aber im Package nicht wegen uns
        )
        
    async def _connect(self):
        if self._connected:
            return
        try:
            await self.producer.start()
            print('Kafka Producer connected')
            self._connected= True
        except Exception as e:
            print("Error happened: ",e)
                
    async def send_message(self, data):
        if not self._connected:
            await self._connect()    
        try:
            await self.producer.send_and_wait(self.topic, data)
            print('Message sent successfully')
        except Exception as e:
            print(f"Error sending message: {e}")
            
    async def send_frame(self, frame_grabber):
        """Capture a frame from FrameGrabber and send to Kafka"""
        frame_bytes = frame_grabber.read_frame()
        if frame_bytes:
            await self.send_message(frame_bytes)
            
    async def disconnect(self):
        if self._connected and self.producer:
            try:
                await self.producer.stop()
            except Exception as e:
                print("Error while disconnecting", e)
        else:
            print("Error cannot disconnect no connection was found ")
            
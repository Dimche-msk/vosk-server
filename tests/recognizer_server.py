import ipaddress
import asyncio
from multiprocessing import Queue
import asyncio_dgram
import datetime,time
from vosk import Model, KaldiRecognizer
import audioop
import auditok
from scapy.all import RTP
import click
import json
import motor.motor_asyncio


model = Model('/home/alex/vosk-server/model')


class DB:
    _db: motor.motor_asyncio.AsyncIOMotorClient

    @classmethod
    async def init(cls, params=None):
        """
        params = {
            'db_url' : 'mongodb://localhost:27017',
            'db_user' : None,
            'db_password' : None,
            }
        """
        if params is None:
            params = {'mongo_url':'mongodb://localhost:27017', 'db_user': None, 'db_password': None}
        cls.db_url = params.get("db_url","mongodb://localhost:27017")
        cls.db_user = params.get("db_user", None)
        cls.db_password = params.get("db_password", None)
        try:
            cls._db = motor.motor_asyncio.AsyncIOMotorClient(cls.mongo_url,username=cls.db_user,password=cls.db_password)
        except Exception as e:
            print(f'Error connection to mongo DB {cls.db_url}. Error={e}')
            quit(1)
        print(cls._db.database_names())

    @classmethod
    async def write_result(cls, result=None,header=None):
        """
        write recognition result to DB
        result = {....} final result
        header = {'date':'datetime session started',
                'source':"IP:port" jf source stream
                'record_path':'/../../xxxxx.wav',
        }
        """


def recognizer_process(queue_audio, queue_text):
    """
    as result: place into queue_text <- (text, True|False)  where:
        text - a str with recognizer result, to json.loads()
    """
    print('Worker started')
    rec = KaldiRecognizer(model, 8000)
    last_received = datetime.datetime.now()
    partial = True
    while True:
        queue_bytes = b''
        while not queue_audio.empty():
            last_received = datetime.datetime.now()
            queue_bytes += queue_audio.get()
        if rec.AcceptWaveform(queue_bytes):
            res = rec.Result()
            partial = False
            queue_text.put(res)

        if datetime.datetime.now()-datetime.timedelta(seconds=60) > last_received:
            if partial:
                queue_text.put(rec.FinalResult())
            print(f'Worker stopped ')
            time.sleep(1)
            return
        time.sleep(1)


class UdpRtpServer:
    connections = dict()

    def __init__(self, addr):
        self._global_data = b''
        self.queue_audio = Queue()
        self.queue_text = Queue()
        self.address = addr[0]
        self.port = addr[1]
        self.text_result = {
            "start_time": datetime.datetime.now(),
            "from_ip": self.address,
            "from_port": self.port,
            "text": "",
            "result":[]
        }
        self._receiver = None
    @classmethod
    async def start_server(cls,bind,port,allow_global):
        stream = await asyncio_dgram.bind((bind,port))
        while True:
            data, addr = await stream.recv()
            if hash(f'{ipaddress.ip_address(addr[0])}:{addr[1]}') not in cls.connections.keys():
                if not allow_global and ipaddress.ip_address(addr[0]).is_global:
                    print(f'Error: attempt to connect from {addr[0]}:{addr[1]} IS RESTRICTED!')
                    continue
                listener = cls(addr)
                cls.connections.update({hash(f'{ipaddress.ip_address(addr[0])}:{addr[1]}'): listener})
                asyncio.create_task(listener.on_new_connection())
                continue
            try:
                listener = cls.connections.get(hash(f'{ipaddress.ip_address(addr[0])}:{addr[1]}'),None)
            except Exception as e:
                print(f'Error: can\'t find link to worker')
                continue
            listener.on_data_received(data)

    async def on_new_connection(self):
        print(f'New connect from {ipaddress.IPv4Address(self.address)}:{self.port}')
        self._receiver = asyncio.create_task(self.text_receiver())
        await asyncio.gather(
            asyncio.to_thread(recognizer_process,self.queue_audio, self.queue_text),
        )
        self._receiver.cancel()
        self.__class__.connections.pop(hash(f'{ipaddress.ip_address(self.address)}:{self.port}'))
        print(f'Job Done! {ipaddress.ip_address(self.address)}:{self.port}, total jobs now:{len(self.__class__.connections)}')
        print(f'Final Result: {self.text_result}')

    def on_data_received(self, data):
        self._global_data += audioop.ulaw2lin(RTP(data).load, 2)  # data.decode()
        audio_regions = auditok.split(
            self._global_data,
            audio_format='bytes',
            sampling_rate=8000,
            sample_width=2,
            channels=1,
            min_dur=0.3,  # minimum duration of a valid audio event in seconds
            max_dur=6,  # maximum duration of an event
            max_silence=0.3,  # maximum duration of tolerated continuous silence within an event
            energy_threshold=50  # threshold of detection
        )
        if len(list(audio_regions)) > 1:
            self.queue_audio.put(self._global_data)
            self._global_data = b''

    async def text_receiver(self):
        while True:
            while not self.queue_text.empty():
                result = self.queue_text.get()
                # write recognized data
                print(f'Received for {result}, Session={self.address}:{self.port}')
                try:
                    dict_result = json.loads(result)
                    self.text_result["text"] += f' {dict_result.get("text")}'
                    self.text_result["result"] += dict_result.get("result")
                except Exception as e:
                    print(f'Error converting recognition result:{e}')
            await asyncio.sleep(1)


@click.command()
@click.option('--allow-global/--no-allow_global', default=False, help='allow to connect from asterisks with "white" addresses ')
@click.option('--bind_address', default="0.0.0.0", help='bind address to listen packets from asterisks')
@click.option('--bind_port', default="8808", help='UDP port to listen voice packets from asterisks')
def main(allow_global,bind_address,bind_port):
    DB.init()
    quit()
    print(f'Starting server on UDP://{bind_address}:{bind_port}')
    loop = asyncio.get_event_loop()

    loop.run_until_complete(asyncio.gather(UdpRtpServer.start_server(bind_address, int(bind_port),allow_global),))


if __name__ == '__main__':
    main()

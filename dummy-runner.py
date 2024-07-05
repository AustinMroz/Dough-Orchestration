import subprocess
import time
import asyncio
import aiohttp
import json

instance_number = 0
class Instance:
    def __init__(self):
        global instance_number
        self.p = subprocess.Popen(['python','dummy-client.py', str(instance_number)])
        instance_number+=1
    def poll(self):
        if self.p.poll():
            self.__init__()

async def manage_instances():
    instances = [Instance() for x in range(1)]
    while True:
        for instance in instances:
            instance.poll()
        await asyncio.sleep(1)

async def send_request(session):
    data = {"prompt": {}, "extra_data": {"remote_files": []}}
    print("sending prompt")
    async with session.post('http://127.0.0.1:8888/prompt', json=data) as resp:
        await resp.text()


async def main():
    mi = asyncio.create_task(manage_instances())
    #wait for dummy clients to connect
    await asyncio.sleep(3)
    async with aiohttp.ClientSession() as session:
        #while True:
        #    await asyncio.sleep(20)
        while True:
            reqs = []
            for i in range(2):
                reqs.append(send_request(session))
            await asyncio.gather(*reqs)

asyncio.run(main())

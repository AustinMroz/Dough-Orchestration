import aiohttp
import asyncio
import sys
import uuid
import signal
import functools
import random
import json

from server import calc_estimated_pixelsteps

wrong_list = [(50, 'nop'), (1,'crash'), (2, 'softexit'), (5, 'outoforder'), (10, 'error')]
wrong_list = [(50, 'nop')]
wrong_sum = functools.reduce(lambda x,y: x+y[0], wrong_list, 0)

files = set()
async def queue_loop(jobqueue):
    gpu_speed = random.randint(2**20,2**22)
    dl_speed = random.randint(5,25)
    while True:
        f,workflow = await jobqueue.get()
        #Simulate file grab
        for file in workflow['extra_data']['remote_files']:
            if file['url'] not in files:
                await asyncio.sleep(random.randint(1,60)/dl_speed)
                files.add(file['url'])
        t = calc_estimated_pixelsteps(workflow) / gpu_speed
        print(t)
        await asyncio.sleep(t)
        #Randomly, some number of things can go wrong
        rand_num = random.randint(0,wrong_sum)
        for item in wrong_list:
            rand_num -= item[0]
            if rand_num <= 0:
                event = item[1]
                break
        match event:
            case 'nop':
                pass
            case 'crash':
                signal.raise_signal(signal.SIGKILL)
            case 'outoforder':
                t1,f1 = await jobqueue.get()
                await asyncio.sleep(t1)
                f1.set_result("gen comple")
            case 'error':
                f.set_result('error')
                continue
            case 'softexit':
                sys.exit()
        f.set_result({"result": f"gen complete: {sys.argv[-1]}"})

async def main():
    jobqueue = asyncio.Queue()
    queueloop = asyncio.create_task(queue_loop(jobqueue))
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect('http://127.0.0.1:8888/ws') as ws:
            try:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        js = msg.json()
                        resp = {"message_id": js.get('message_id', 0)}
                        match js['command']:
                            case 'prompt':
                                f = asyncio.Future()
                                jobqueue.put_nowait((f, js['data']))
                                resp['data'] = await f
                            case "queue":
                                resp['data'] = [[],[]]
                            case "files":
                                resp['data'] = []
                            case "info":
                                resp['data'] = {'machine_id': str(uuid.uuid4())}
                            case "logs":
                                resp['data'] = "no logs"
                            case _:
                                resp = {"error": "Unknown command"}
                        await ws.send_json(resp)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        print("recieved error message")
            except Exception as e:
                print(e)

asyncio.run(main())

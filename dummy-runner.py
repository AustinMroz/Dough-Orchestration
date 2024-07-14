import subprocess
import time
import asyncio
import aiohttp
import json
import random
import argparse

instance_number = 0


class Instance:
    def __init__(self):
        global instance_number
        self.p = subprocess.Popen(["python", "dummy-client.py", str(instance_number)])
        instance_number += 1

    def poll(self):
        if self.p.poll():
            self.__init__()


async def manage_instances():
    print("dummy cluster active---")
    instances = [Instance() for x in range(2)]
    while True:
        for instance in instances:
            instance.poll()
        await asyncio.sleep(1)


async def send_request(session):
    inputs = {
        "width": random.randint(256, 1024),
        "height": random.randint(256, 1024),
        "steps": random.randint(5, 40),
    }
    data = {"prompt": {"1": {"inputs": inputs}}, "extra_data": {"remote_files": []}}
    async with session.post("http://127.0.0.1:8888/prompt", json=data) as resp:
        await resp.text()


async def main():
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("--simulate_tasks", action="store_true", help="Simulate tasks")
    args = parser.parse_args()
    if args.simulate_tasks:
        mi = asyncio.create_task(manage_instances())
        # wait for dummy clients to connect
        await asyncio.sleep(3)
        async with aiohttp.ClientSession() as session:
            # while True:
            #    await asyncio.sleep(20)
            while True:
                reqs = []
                for i in range(4):
                    reqs.append(send_request(session))
                await asyncio.gather(*reqs)
    else:
        await manage_instances()


asyncio.run(main())

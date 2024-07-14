import aiohttp
import asyncio
import sys
import uuid
import signal
import functools
import random
import json

from utils.job import calc_estimated_pixelsteps

wrong_list = [
    (50, "nop"),
    (1, "crash"),
    (2, "softexit"),
    (5, "outoforder"),
    (10, "error"),
]
# wrong_list = [(50, 'nop')]
wrong_sum = functools.reduce(lambda x, y: x + y[0], wrong_list, 0)

files = set()


async def queue_loop(jobqueue):
    job_number = 0
    gpu_speed = random.randint(2**20, 2**22)
    dl_speed = random.randint(5, 25)
    while True:
        try:
            f, input_params = await jobqueue.get()
            input_params = json.loads(input_params)
            # @austin we will need to plug these in
            try:
                workflow = input_params["workflow_input"]
                file_path_list = input_params["file_path_list"]
                output_node = input_params["output_node"]
                extra_model_list = input_params["extra_model_list"]
                ignore_model_list = input_params["ignore_model_list"]
                extra_node_urls = input_params["extra_node_urls"]
                comfy_commit_hash = input_params["comfy_commit_hash"]
            except Exception as e:
                raise Exception(f"error parsing input {str(e)}")

            # Simulate file grab
            print("filepath list: ", file_path_list)
            for file in file_path_list:
                fp = file["filepath"] if "filepath" in file else file
                if fp not in files:
                    await asyncio.sleep(random.randint(1, 60) / dl_speed)
                    files.add(fp)

            print("here")
            t = calc_estimated_pixelsteps(workflow) / gpu_speed
            print(t)
            await asyncio.sleep(t)
            # Randomly, some number of things can go wrong
            rand_num = random.randint(0, wrong_sum)
            for item in wrong_list:
                rand_num -= item[0]
                if rand_num <= 0:
                    event = item[1]
                    break
            if event != "nop":
                print(event)
            match event:
                case "nop":
                    pass
                case "crash":
                    signal.raise_signal(signal.SIGKILL)
                case "outoforder":
                    if not jobqueue.empty():
                        t1, f1 = await jobqueue.get()
                        await asyncio.sleep(t1)
                        response = get_response(t1, job_number)
                        job_number += 1
                        f1.set_result("response")
                case "error":
                    f.set_result("error")
                    continue
                case "softexit":
                    sys.exit()
            response = get_response(t, job_number)
            job_number += 1
            f.set_result(response)
        except Exception as e:
            print(f"exception occured: {str(e)}")


def get_response(t, job_number):
    print("sending response.....")
    return {
        "prompt_id": str(uuid.uuid4()),
        "number": job_number,
        "node_errors": {},
        "outputs": [f"output/ComfyUI_{job_number:05d}_.png"],
        "execution_time": t,
        "machineid": str(sys.argv[-1]),
    }


async def main():
    jobqueue = asyncio.Queue()
    queueloop = asyncio.create_task(queue_loop(jobqueue))
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect("http://127.0.0.1:8888/ws") as ws:
            try:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        js = msg.json()
                        resp = {"message_id": js.get("message_id", 0)}
                        match js["command"]:
                            case "prompt":
                                f = asyncio.Future()
                                print("message received: ", js["command"], js["data"])
                                jobqueue.put_nowait((f, js["data"]))
                                resp["data"] = await f
                            case "queue":
                                resp["data"] = [[], []]
                            case "files":
                                resp["data"] = []
                            case "info":
                                resp["data"] = {"machine_id": str(sys.argv[-1])}
                            case "logs":
                                resp["data"] = "no logs"
                            case _:
                                resp = {"error": "Unknown command"}
                        await ws.send_json(resp)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        print("recieved error message")
            except Exception as e:
                print(e)


asyncio.run(main())

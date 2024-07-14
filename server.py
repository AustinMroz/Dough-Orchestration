import json
import os
import asyncio
import time
import aiohttp
from aiohttp import web
import ssl
import hashlib

import requests

from constants import INF_BACKEND_URL, STATIC_AUTH_TOKEN, InferenceStatus
from utils.job import Job
from utils.response import success


async def query(socket, command):
    # TODO: implement centralized read loop in case a socket has multiple waiters
    await socket.send_json({"command": command})
    res = await anext(socket, None)
    return res


class Worker:
    def __init__(self, socket, machine_id):
        self.queue = []
        self.socket = socket
        self.machine_id = machine_id
        self.messages = {}
        self.message_id = 1
        # TODO: measure per container values
        # NOTE: Maybe possible. would need to check
        # self.dl_speed = container_size/creation_time
        self.dl_speed = 2**22
        self.exp_itss_per_pixel = 2**21

    def estimate_dl_time(self, files):
        size = 0
        # TODO: wrap files as class, etag/hash support
        for file in files:
            # Make a series of rough guesses
            match file:
                case x if x.startswith("models/checkpoints"):
                    size += 2**32  # 4GB
                case x if x.startswith("models"):
                    size += 2**30  # 1GB
                case x if x.split(".")[-1] in ["mp4", "webm", "mkv", "gif"]:
                    size += 2**28
                case x if x.split(".")[-1] in ["png", "jpg", "jpeg"]:
                    size += 2**22
                case _:
                    size += 2**24
        return size / self.dl_speed

    async def update_filelist(self):
        resp = await (await self.send_command({"command": "files"}))
        self.files = resp["data"]

    def recursive_estimate_time(self, job, ind=None):
        if ind is None:
            ind = len(self.queue)
        else:
            job = self.queue[ind]
        exec_time = self.estimate_execution_time(job)
        if ind == 0:
            # TODO subtract current execution time?
            sub_dur = 0
            dl_slack = 0
            avail_files = self.files

        else:
            sub_dur, dl_slack, avail_files = self.recursive_estimate_time(job, ind - 1)

        dl_time = self.estimate_dl_time(set(job.assets).difference(avail_files))
        # NOTE: This may make dl_time negative
        dl_time -= dl_slack
        return (
            sub_dur + max(exec_time, dl_time),
            max(exec_time - dl_time, 0),
            set(job.assets).union(self.files),
        )

    def estimate_execution_time(self, job):
        return job.estimated_pixelsteps / self.exp_itss_per_pixel

    async def recieve_response(self):
        while True:
            resp = await anext(self.socket, None)
            if resp is None:
                # Connection closed. Mark all in flight messages as dead
                for message_future in self.messages.values():
                    message_future.set_result({"error": "connection closed"})

                # TODO: reconsider try-catch if returning gracefully
                break
            resp = resp.json()
            print(resp)
            if "error" not in resp:
                future = self.messages.pop(resp["message_id"])
                future.set_result(resp)

    async def send_command(self, command):
        future = asyncio.Future()
        command["message_id"] = self.message_id
        self.messages[self.message_id] = future
        self.message_id += 1
        await self.socket.send_json(command)

        return future

    async def queue_job(self, job: Job):
        self.queue.append(job)
        f = await self.send_command({"command": "prompt", "data": job.to_json()})

        def on_finish(f, job=job):
            self.queue.remove(job)
            # update speed estimates
            resp = f.result()
            # NOTE: first job completed is 50% decay
            if "data" not in resp or "number" not in resp["data"]:
                return
            weight = min(resp["data"]["number"] + 2, 10)
            measured = job.estimated_pixelsteps / float(resp["data"]["execution_time"])
            self.exp_itss_per_pixel = (
                self.exp_itss_per_pixel * (weight - 1) / weight + measured / weight
            )

        f.add_done_callback(on_finish)
        return f


class WorkerBatch:
    def __init__(self, loop):
        self.workers = []
        self.incomplete_jobs = []
        self.has_incomplete_jobs = asyncio.Event()
        self.r_loop = loop.create_task(self.result_loop())

    async def connect_socket(self, socket):
        machine_id = (await query(socket, "info")).json()["data"]["machine_id"]
        for worker in self.workers:
            if worker.machine_id == machine_id:
                worker.socket = socket
                break
        else:
            worker = Worker(socket, machine_id)
            resp = (await query(socket, "files")).json()
            worker.files = resp["data"]
            self.workers.append(worker)
        try:
            await worker.recieve_response()
        except:
            print("disconnected:")
        worker.socket = None
        await asyncio.sleep(120)
        if worker.socket is None:
            print("pruned worker" + worker.machine_id)
            self.workers.remove(worker)

    async def queue_job(self, job):
        active_workers = list(filter(lambda w: w.socket is not None, self.workers))
        while len(active_workers) == 0:
            # @austin: raising exception here
            raise Exception("no workers available")
            # await asyncio.sleep(5)
            # active_workers = list(filter(lambda w: w.socket is not None, self.workers))
        w = min(active_workers, key=lambda w: w.recursive_estimate_time(job)[0])
        future = await w.queue_job(job)
        future.job = job
        self.incomplete_jobs.append(future)
        self.has_incomplete_jobs.set()
        return future

    async def result_loop(self):
        while True:
            await self.has_incomplete_jobs.wait()
            # Pending must be discarded, new jobs may have been added
            done, _ = await asyncio.wait(
                self.incomplete_jobs, timeout=30, return_when=asyncio.FIRST_COMPLETED
            )
            if len(done) == len(self.incomplete_jobs):
                self.has_incomplete_jobs.clear()
            to_notify = []
            to_requeue = []
            for f in done:
                resp = f.result()
                job = f.job
                self.incomplete_jobs.remove(f)
                # NOTE: queues must be delayed to keep incomplete job modifications atomic
                # TODO: standardize error messaging
                if "error" in resp or "error" in resp["data"]:
                    to_requeue.append(self.queue_job(job))
                else:
                    data = resp["data"]
                    to_notify.append(
                        (
                            data["outputs"],
                            job.jobid,
                            data["execution_time"],
                            data["machineid"],
                        )
                    )
            if len(to_requeue) > 0:
                await asyncio.gather(*to_requeue)
            for n in to_notify:
                pass
                self.notify_finish(*n)

    def notify_finish(self, *args):
        # print("******* output generated: ", args)
        # TODO: make an http interface if more api calls are involved
        headers = {
            "Authorization": f"Bearer {STATIC_AUTH_TOKEN}",
            "Content-Type": "application/json",
        }

        data = json.dumps(
            {
                "uuid": args[1],  # jobid
                "output_details": args[0][0],  # data["outputs"]
                "total_inference_time": args[2],  # data["execution_time"],
                "status": (
                    InferenceStatus.COMPLETED.value
                    if len(args[0])
                    else InferenceStatus.FAILED.value
                ),
            }
        )
        response = requests.put(
            INF_BACKEND_URL + "/v1/inference/log",
            data=data,
            headers=headers,
        )
        if response.status_code == 200:
            data = response.json()
            print("log updated successfully: ", data)
        else:
            print(f"log update failed: {response.status_code} - {response.text}")

    async def websocket_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        print("connected")
        # NOTE: Never returns
        await self.connect_socket(ws)
        return ws

    async def post_prompt(self, request):
        try:
            js = await request.json()
            input_params, log_uuid = js["input_params"], js["log_uuid"]
            input_params = json.loads(input_params)
        except Exception as e:
            return success({}, f"error parsing request {str(e)}", False)

        # @austin: i added a wrapper for sending the response + a retry loop
        completed = None
        retry_count = 5
        while retry_count > 0:
            try:
                # @austin: i was not really sure on how wid and jobid differ
                # print("input params: ", input_params)
                job = Job(
                    workflow_input=input_params["workflow_input"],
                    wid=log_uuid,
                    jobid=log_uuid,
                    file_path_list=input_params.get("file_path_list", None),
                    output_node=input_params.get("output_node", None),
                    extra_model_list=input_params.get("extra_model_list", None),
                    ignore_model_list=input_params.get("ignore_model_list", None),
                    extra_node_urls=input_params.get("extra_node_urls", None),
                    comfy_commit_hash=input_params.get("comfy_commit_hash", None),
                )
                completed = await self.queue_job(job)
                break
            except Exception as e:
                print(f"Error queueing job.. retrying... {str(e)}")

            time.sleep(1)
            retry_count -= 1

        if not completed:
            return success({}, "no workers available", False)
        else:
            # data = await completed
            return success({}, "job queued successfully", True)

    async def dump_logs(self, request):
        output = []
        for worker in self.workers:
            try:
                output.append(await (await worker.send_command({"command": "logs"})))
            except Exception as e:
                output.append("failed: " + str(e))
        return success(output, "output fetched successfully", True)


# Framework code for easier testing
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    workerbatch = WorkerBatch(loop)

    async def start_server():
        # Needs further testing. Connections were valid from browser, but not python
        # context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        # context.load_cert_chain(serverCert, serverKey)
        context = None
        app = web.Application()
        app.add_routes([web.get("/ws", workerbatch.websocket_handler)])
        app.add_routes([web.post("/prompt", workerbatch.post_prompt)])
        app.add_routes([web.get("/dump_logs", workerbatch.dump_logs)])
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "", 8888, ssl_context=context)
        await site.start()
        print("server started")

    task = loop.create_task(start_server())
    loop.run_forever()

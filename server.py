import asyncio
import aiohttp
from aiohttp import web
import ssl
import hashlib

def get_headers():
    return {'Salad-Api-Key': os.environ['SALAD_API_KEY']}

def file_hash(data):
    #NOTE, to facilitate hashing of zipped files, this variant is on data
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()
async def query(socket, command):
    #TODO: implement centralized read loop in case a socket has multiple waiters
    await socket.send_json({'command': command})
    res = await anext(socket, None)
    return res


class Worker:
    def __init__(self, socket, machine_id):
        self.queue = []
        self.socket = socket
        self.machine_id = machine_id
        self.messages = {}
        self.message_id = 1
        #TODO: measure per container values
        #NOTE: Maybe possible. would need to check
        #self.dl_speed = container_size/creation_time
        self.dl_speed = 2 ** 22
        self.exp_itss_per_pixel = 2**21
    def estimate_dl_time(self, files):
        size = 0
        #TODO: wrap files as class, etag/hash support
        for file in files:
            #Make a series of rough guesses
            match file:
                case x if x.startswith('models/checkpoints'):
                    size += 2 ** 32 #4GB
                case x if x.startswith('models'):
                    size += 2 ** 30 #1GB
                case x if x.split('.')[-1] in ['mp4','webm','mkv', 'gif']:
                    size += 2 ** 28
                case x if x.split('.')[-1] in ['png', 'jpg', 'jpeg']:
                    size += 2 ** 22
                case _:
                    size += 2 ** 24
        return size / self.dl_speed
    async def update_filelist(self):
        resp = await (await self.send_command({'command': 'files'}))
        self.files = resp['data']

    def recursive_estimate_time(self, job, ind=None):
        if ind is None:
            ind = len(self.queue)
        else:
            job = self.queue[ind]
        exec_time = self.estimate_execution_time(job)
        if ind == 0:
            #TODO subtract current execution time?
            sub_dur = 0
            dl_slack = 0
            avail_files = self.files

        else:
            sub_dur, dl_slack, avail_files = self.recursive_estimate_time(job, ind-1)
        dl_time = self.estimate_dl_time(job.assets.difference(avail_files))
        #NOTE: This may make dl_time negative
        dl_time -= dl_slack
        return  sub_dur + max(exec_time, dl_time), max(exec_time-dl_time,0), job.assets.union(self.files)
    def estimate_execution_time(self, job):
        return  job.estimated_pixelsteps /self.exp_itss_per_pixel
    async def recieve_response(self):
        while True:
            resp = await anext(self.socket, None)
            if resp is None:
                break
            resp = resp.json()
            print(resp)
            if 'error' not in resp:
                self.messages.pop(resp['message_id']).set_result(resp)
    async def send_command(self, command):
        future = asyncio.Future()
        command['message_id'] = self.message_id
        self.messages[self.message_id] = future
        self.message_id += 1
        await self.socket.send_json(command)

        return future
    async def queue_job(self, job):
        self.queue.append(job)
        f = await self.send_command({'command': 'prompt', 'data': job.workflow})
        f.add_done_callback(lambda x: self.queue.remove(job))
        return f


class WorkerBatch:
    def __init__(self, loop):
        self.workers = []
        self.incomplete_jobs = []
        self.has_incomplete_jobs = asyncio.Event()
        self.r_loop = loop.create_task(self.result_loop())
    async def connect_socket(self, socket):
        machine_id = (await query(socket, "info")).json()['data']['machine_id']
        for worker in self.workers:
            if worker.machine_id == machine_id:
                worker.socket = socket
                break
        else:
            worker = Worker(socket, machine_id)
            resp =(await query(socket, 'files')).json()
            worker.files = resp['data']
            self.workers.append(worker)
        try:
            await worker.recieve_response()
        except:
            print('disconnected:')
        worker.socket = None
        await asyncio.sleep(120)
        if worker.socket is None:
            print('pruned worker' + worker.machine_id)
            self.workers.remove(worker)
    async def queue_job(self, job):
        active_workers = list(filter(lambda w: w.socket is not None, self.workers))
        if len(active_workers) == 0:
            f = asyncio.Future()
            f.set_result("No available workers")
            return f
        w =min(active_workers, key=lambda w: w.recursive_estimate_time(job)[0])
        future = await w.queue_job(job)
        self.incomplete_jobs.append(future)
        self.has_incomplete_jobs.set()
        return future
    async def result_loop(self):
        while True:
            await self.has_incomplete_jobs.wait()
            #Pending must be discarded, new jobs may have been added
            done, pending = await asyncio.wait(self.incomplete_jobs, timeout=30, return_when=FIRST_COMPLETED)
            if len(pending) == 0:
                self.has_incomplete.clear()
            to_notify = []
            for resp, job in done:
                if "error" in resp:
                    await self.queue_job(job)
                    continue
                self.incomplete_jobs.remove(job)
                to_notify.append((resp['output'], job.id, resp['execution_time'], resp['machineid']))
            for n in to_notify:
                pass
                #notify_finish(*n)

class Job:
    def __init__(self, workflow, assets, wid, jobid):
        self.workflow = workflow
        self.estimated_pixelsteps = calc_estimated_pixelsteps(workflow)
        #TODO: Check assets form, begin upload if needed, (invert)
        self.assets = set([file_hash(f['url'].encode('utf-8')) for f in workflow['extra_data']['remote_files']])
def calc_estimated_pixelsteps(workflow):
    steps = 0
    resolution = []
    for node in workflow['prompt'].values():
        width, height = 0,0
        for ik in node['inputs']:
            if not isinstance(node['inputs'][ik], int):
                continue
            if ik == 'steps':
                steps += node['inputs'][ik]
            elif ik == 'width':
                width = node['inputs'][ik]
            elif ik == 'height':
                height = node['inputs'][ik]
        if width and height:
            resolution.append(width * height)
    if len(resolution) > 0:
        avg_size = sum(resolution)/len(resolution)
    else:
        avg_size = 1920 * 1080 * 16
    steps = steps or 20
    #TODO: cache value per job?
    return steps*avg_size


#messagequeue = asyncio.Queue()
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    print('connected')
    #NOTE: Never returns
    await workerbatch.connect_socket(ws)
    return ws
jobs = []
async def post_prompt(request):
    js = await request.json()
    processing = asyncio.Future()
    #workerbatch.queue_job(Job(js['workflow'], js['assets'], js['wid'], js['jobid']))
    completed  = await workerbatch.queue_job(Job(js,None, None, None))
    return web.json_response(await completed)
    #await messagequeue.put((js, processing, completed))
    ind = len(jobs)
    jobs.append((processing, completed))
    return web.json_response({'job_id': ind})
async def get_job_status(request):
    js = await request.json()
    if 'job_ids' in js:
        target_jobs = [jobs[i] for i in js['job_ids']]
    else:
        target_jobs = jobs.copy()
    for i in range(len(target_jobs)):
        if target_jobs[i][1].done():
            target_jobs[i] = target_jobs[i][1].result()
        else:
            target_jobs[i] = "Processing" if target_jobs[i][0].done() else "In queue"
    return web.json_response(target_jobs)
async def dump_logs(request):
    output = []
    for worker in workerbatch.workers:
        try:
            output.append(await (await worker.send_command({'command': 'logs'})))
        except Exception as e:
            output.append("failed: " + str(e))
    return web.json_response(output)

#Framework code for easier testing
if __name__ == "__main__":
    async def start_server():
        #Needs further testing. Connections were valid from browser, but not python
        #context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        #context.load_cert_chain(serverCert, serverKey)
        context=None


        app = web.Application()
        app.add_routes([web.get('/ws', websocket_handler)])
        app.add_routes([web.post('/prompt', post_prompt)])
        app.add_routes([web.post('/job_status', get_job_status)])
        app.add_routes([web.get('/dump_logs', dump_logs)])
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '', 8888, ssl_context=context)
        await site.start()
        print("server started")

    loop = asyncio.new_event_loop()
    workerbatch = WorkerBatch(loop)
    task = loop.create_task(start_server())
    loop.run_forever()
else:
    loop = asyncio.get_event_loop()
    workerbatch = WorkerBatch(loop)

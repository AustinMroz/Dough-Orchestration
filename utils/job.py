import hashlib
import json

from constants import SALAD_API_KEY


def get_headers():
    return {"Salad-Api-Key": SALAD_API_KEY}


def file_hash(data):
    # NOTE, to facilitate hashing of zipped files, this variant is on data
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


class Job:
    def __init__(
        self,
        workflow_input,
        wid,
        jobid,
        file_path_list=None,
        output_node=None,
        extra_model_list=None,
        ignore_model_list=None,
        extra_node_urls=None,
        comfy_commit_hash=None,
    ):
        self.workflow_input = workflow_input
        self.jobid = jobid
        self.wid = wid
        self.estimated_pixelsteps = calc_estimated_pixelsteps(workflow_input)
        # TODO: Check assets form, begin upload if needed, (invert)
        self.assets = list(
            set([file_hash(f["url"].encode("utf-8")) for f in file_path_list])
        )
        # TODO: @austin these also need to be plugged in, let me know how to handle these
        """
        file_path_list is an array. the elements can either be just strings ["abc.com", "xyz.com"..]
        or can be a dict 
        {
            "filepath": "abc.com", # assuming remote url
            "dest_folder": "/control_imgs"  # sub-folder within the input folder
        }
        """
        self.file_path_list = file_path_list
        self.stop_server_after_completion = False
        self.output_node = output_node
        self.extra_model_list = extra_model_list
        self.ignore_model_list = ignore_model_list
        self.extra_node_urls = extra_node_urls
        self.comfy_commit_hash = comfy_commit_hash

    def to_json(self):
        return json.dumps(self.__dict__)


def calc_estimated_pixelsteps(workflow):
    print("workflow..... ", workflow)
    steps = 0
    resolution = []
    for node in workflow.values():
        width, height = 0, 0
        for ik in node["inputs"]:
            if not isinstance(node["inputs"][ik], int):
                continue
            if ik == "steps":
                steps += node["inputs"][ik]
            elif ik == "width":
                width = node["inputs"][ik]
            elif ik == "height":
                height = node["inputs"][ik]
        if width and height:
            resolution.append(width * height)

    if len(resolution) > 0:
        avg_size = sum(resolution) / len(resolution)
    else:
        avg_size = 1920 * 1080 * 16
    steps = steps or 20
    # TODO: cache value per job?
    return steps * avg_size

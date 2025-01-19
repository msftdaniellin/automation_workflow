import uuid
import time
from rq import Queue
from redis import Redis
from st2common.runners.base_action import Action

__all__ = ["Common_python_task"]

redis_conn = Redis(host='localhost', port=6379, db=0)
queue = Queue('default', connection=redis_conn)


class Common_python_task(Action):
    def run(self, task, extra_args, extra_kwargs):
        if extra_args and extra_kwargs:
            job_id = self.submit_job(task, *extra_args, **extra_kwargs)
        elif not extra_kwargs and extra_args:
            job_id = self.submit_job(task, *extra_args)
        elif extra_kwargs and not extra_args:
            job_id = self.submit_job(task, *extra_args)
        else:
            job_id = self.submit_job(task)
        return self.get_job_result(job_id)

    def submit_job(self, full_function_name, *args, **kwargs):
        job_id = str(uuid.uuid4())
        job = queue.enqueue('Worker.run_function',full_function_name, *args, **kwargs, job_id=job_id)
        return job.id
    
    def get_job_result(self, job_id):
        job = queue.fetch_job(job_id)
        for i in range(60):
            if job is None:
                return (False, {'error': f'Job id {job_id} failed.'})
            elif job.is_finished:
                return job.result
            elif job.is_failed:
                return (False, {'error': f'Job id {job_id} failed.'})
            else:
                time.sleep(1)
        return (False, {'error': f'Job id {job_id} timeout.'})

import json
import os
import time
from enum import Enum
from typing import Dict, List, Optional

from kubernetes import watch
from kubernetes.client.models.v1_pod import V1Pod
from kubernetes.client.rest import ApiException
from loguru import logger

from pycalrissian.context import CalrissianContext
from pycalrissian.job import CalrissianJob, ContainerNames
from pycalrissian.utils import copy_from_volume


class JobStatus(Enum):
    ACTIVE = "active"
    FAILED = "failed"
    SUCCEEDED = "succeeded"
    KILLED = "killed"


class CalrissianExecution:
    def __init__(self, job: CalrissianJob, runtime_context: CalrissianContext) -> None:
        self.job = job
        self.runtime_context = runtime_context
        self.namespaced_job = None
        self.killed = False

    def submit(self):
        """Submits the job to the cluster"""
        logger.info(f"submit job {self.job.job_name}")
        response = self.runtime_context.batch_v1_api.create_namespaced_job(
            self.runtime_context.namespace, self.job.to_k8s_job()
        )
        self.namespaced_job_name = self.job.job_name
        self.namespaced_job = response
        logger.info(f"job {self.job.job_name} submitted")

    def get_status(self):
        """Returns the job status"""
        if self.killed:
            return JobStatus.KILLED
        try:
            response = self.runtime_context.batch_v1_api.read_namespaced_job_status(
                name=self.namespaced_job_name,
                namespace=self.runtime_context.namespace,
                pretty=True,
            )
            if response.status.active is None and response.status.start_time is None:
                return JobStatus.ACTIVE
            if response.status.active:
                return JobStatus.ACTIVE
            if response.status.succeeded:
                return JobStatus.SUCCEEDED
            if response.status.failed:
                return JobStatus.FAILED
            return None
        except ApiException as e:
            logger.error(f"Exception when calling get status: {e}\n")
            raise e

    def is_complete(self) -> bool:
        """Returns True if the job execution is completed (success or failed)"""
        return self.get_status() in [
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.KILLED,
        ]

    def is_succeeded(self) -> bool:
        """Returns True if the job execution is completed and succeeded"""
        return self.get_status() in [JobStatus.SUCCEEDED]

    def is_active(self) -> bool:
        """Returns True if the job execution is on-going"""
        return self.get_status() in [JobStatus.ACTIVE]

    def get_output(self) -> Dict:
        """Returns the job output"""
        if self.is_succeeded:
            filename = self.get_file_from_volume(["output.json"])[0]
            with open(filename, "r") as staged_file:
                return json.load(staged_file)

    def get_usage_report(self) -> Dict:
        """Returns the job usage report"""
        if self.is_complete:
            try:
                filename = self.get_file_from_volume(["report.json"])[0]
                with open(filename, "r") as staged_file:
                    return json.load(staged_file)
            except json.decoder.JSONDecodeError:
                return {}

    def get_file_from_volume(self, filenames):

        volume = {
            "name": self.job.volume_calrissian_wdir,
            "persistentVolumeClaim": {
                "claimName": self.runtime_context.calrissian_wdir
            },
        }
        volume_mount = {
            "name": self.job.volume_calrissian_wdir,
            "mountPath": self.job.calrissian_base_path,
        }

        destination_path = "."
        copy_from_volume(
            context=self.runtime_context,
            volume=volume,
            volume_mount=volume_mount,
            source_paths=[
                os.path.join(self.job.calrissian_base_path, filename)
                for filename in filenames
            ],
            destination_path=destination_path,
        )

        return [os.path.join(destination_path, filename) for filename in filenames]

    def get_log(self):
        """Returns the job execution log"""
        if self.is_complete:
            return self._get_container_log(ContainerNames.CALRISSIAN)
        return None

    def get_tool_logs(self):
        """stages the tool logs from k8s volume"""
        usage_report = self.get_usage_report()
        if "children" in usage_report.keys():
            self.get_file_from_volume(
                [
                    os.path.join(self.job.calrissian_base_path, tool["name"] + ".log")
                    for tool in usage_report["children"]
                ]
            )

            return [
                os.path.join(".", tool["name"] + ".log")
                for tool in usage_report["children"]
            ]

    def _get_container_log(self, container):

        try:

            pod_label_selector = f"job-name={self.job.job_name}"
            pods_list = self.runtime_context.core_v1_api.list_namespaced_pod(
                namespace=self.runtime_context.namespace,
                label_selector=pod_label_selector,
                timeout_seconds=10,
            )
            pod_name = pods_list.items[0].metadata.name

            return self.runtime_context.core_v1_api.read_namespaced_pod_log(
                name=pod_name,
                namespace=self.runtime_context.namespace,
                _return_http_data_only=True,
                _preload_content=False,
                container=container.value,
            ).data.decode("utf-8")

        except ApiException as e:
            logger.error(f"Exception when calling get status: {e}\n")
            raise e

    def get_start_time(self):
        """Returns the start time"""
        try:
            response = self.runtime_context.batch_v1_api.read_namespaced_job_status(
                name=self.namespaced_job_name,
                namespace=self.runtime_context.namespace,
                pretty=True,
            )
            if response.status.start_time is not None:
                return response.status.start_time
            return None
        except ApiException as e:
            logger.error(f"Exception when calling get status: {e}\n")
            raise e

    def get_completion_time(self):
        """Returns either the completion time or the last transition time"""
        try:
            response = self.runtime_context.batch_v1_api.read_namespaced_job_status(
                name=self.namespaced_job_name,
                namespace=self.runtime_context.namespace,
                pretty=True,
            )
            if response.status.completion_time is not None:
                return response.status.completion_time
            if response.status.conditions is not None:
                return response.status.conditions[0].last_transition_time
            return None
        except ApiException as e:
            logger.error(f"Exception when calling get status: {e}\n")
            raise e

    def _kill_job(self, reason: str, wall_time: Optional[int] = None) -> None:
        """Helper to kill a job and mark execution as killed."""
        logger.warning(reason)
        self.killed = True
        self.runtime_context.batch_v1_api.delete_namespaced_job(
            namespace=self.runtime_context.namespace,
            name=self.namespaced_job_name,
            timeout=wall_time,
        )

    def monitor(
        self, interval: int = 5, grace_period=120, wall_time: Optional[int] = None
    ) -> None:
        """Monitors job execution using Kubernetes Watch API for event-driven updates.

        Instead of blindly sleeping between status checks, this method uses the
        Kubernetes Watch API to stream Job events in real-time. The watch is
        notified immediately when a Job's status changes (e.g., pod scheduled,
        container started, job completed/failed), eliminating unnecessary delays.

        A timeout on the watch stream ensures that grace_period and wall_time
        checks (e.g., ImagePullBackOff detection) are still performed periodically.

        Args:
            interval: Timeout (in seconds) for each watch iteration. Controls how
                often grace_period/wall_time checks are performed when no events
                arrive. Defaults to 5.
            grace_period: Time (in seconds) after which pods in ImagePullBackOff
                state trigger job termination. Defaults to 120.
            wall_time: Maximum total execution time (in seconds). If set, the job
                is killed when this time is exceeded. Defaults to None (no limit).
        """
        if not self.is_active():
            logger.warning("job is not submitted")
            return

        start_time = time.monotonic()
        w = watch.Watch()

        try:
            while True:
                # Check wall time before starting a new watch cycle
                elapsed = time.monotonic() - start_time
                if wall_time is not None and elapsed > wall_time:
                    self._kill_job(
                        "reached wall time for execution, killing job",
                        wall_time=wall_time,
                    )
                    return

                # Check for ImagePullBackOff after grace period
                if elapsed > grace_period:
                    waiting_pods = self.get_waiting_pods()
                    if waiting_pods:
                        self._kill_job(
                            "found pods in waiting status with reason "
                            "ImagePullBackOff, killing job",  # noqa: E501
                            wall_time=wall_time,
                        )
                        return

                # Calculate remaining timeout for this watch cycle
                remaining_wall = None
                if wall_time is not None:
                    remaining_wall = max(1, int(wall_time - elapsed))
                timeout = min(interval, remaining_wall) if remaining_wall else interval

                logger.info(f"job {self.job.job_name} is active")

                # Use Kubernetes Watch API to stream Job events.
                # The watch will return immediately when a status change occurs
                # (e.g., pod scheduled, succeeded, failed) instead of waiting
                # for the full timeout. If no event arrives within `timeout`
                # seconds, the stream ends and we loop back for periodic checks.
                try:
                    for event in w.stream(
                        self.runtime_context.batch_v1_api.list_namespaced_job,
                        namespace=self.runtime_context.namespace,
                        field_selector=f"metadata.name={self.namespaced_job_name}",
                        timeout_seconds=timeout,
                    ):
                        job_obj = event["object"]
                        status = job_obj.status

                        logger.debug(
                            f"watch event: type={event['type']}, "
                            f"active={status.active}, "
                            f"succeeded={status.succeeded}, "
                            f"failed={status.failed}"
                        )

                        # Job completed or failed — stop watching immediately
                        if status.succeeded or status.failed:
                            w.stop()
                            break

                except ApiException as e:
                    # If watch fails (e.g., 410 Gone due to resource version),
                    # fall back to a short sleep before retrying
                    logger.warning(
                        f"watch stream interrupted, retrying: {e}"
                    )
                    time.sleep(min(interval, 2))

                # After the watch stream ends, do a final status check
                if self.is_complete():
                    break

        finally:
            w.stop()

        if self.is_complete():
            logger.info("execution is complete")
        if self.is_succeeded():
            logger.info("the outcome is: success!")

    def get_waiting_pods(self) -> List[V1Pod]:

        pods_waiting = []

        response = self.runtime_context.core_v1_api.list_namespaced_pod(
            self.runtime_context.namespace
        )

        if response is not None:
            for pod in response.items:
                if pod.status.container_statuses:
                    for con_status in pod.status.container_statuses:

                        if (
                            con_status.state.waiting
                            and con_status.state.waiting.reason in ["ImagePullBackOff"]
                        ):
                            pods_waiting.append(pod)

        return pods_waiting

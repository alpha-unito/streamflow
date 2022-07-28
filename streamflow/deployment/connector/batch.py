import boto3

import asyncio
from asyncio.subprocess import STDOUT
from typing import MutableSequence, MutableMapping, Any, Tuple, Optional, Union

from cachetools import TTLCache

from streamflow.core import utils
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import Location
from streamflow.deployment.connector.base import BaseConnector
from streamflow.log_handler import logger

class AWSBatchConnector(BaseConnector):
    def __init__(self,
                 deployment_name: str,
                 streamflow_config_dir: str,
                 jobQueue: str,
                 attempts: Optional[int] = None,
                 image: Optional[str] = None,
                 jobDefinition: Optional[str] = None,
                 jobName: Optional[str] = None,
                 memory: Optional[int] = None,
                 mountPoints: Optional[MutableSequence[Any]] = None,
                 pollingInterval: int = 15,
                 profileName: Optional[str] = None,
                 regionName: Optional[str] = None,
                 timeout: Optional[int] = None,
                 transferBufferSize: int = 2 ** 16,
                 vcpus: Optional[int] = None,
                 volumes: Optional[MutableSequence[Any]] = None) -> None:
        super().__init__(
            deployment_name=deployment_name,
            streamflow_config_dir=streamflow_config_dir,
            transferBufferSize=transferBufferSize)
        self.attempts: Optional[int] = attempts
        self.image: Optional[str] = image
        if jobDefinition is not None:
            if image is not None:
                logger.info("Image supplied, but could be overridden by job definition")
            if mountPoints is not None or volumes is not None:
                logger.info("Volumes have been supplied but will be ignored")
        if jobDefinition is None:
            if image is None or vcpus is None or memory is None:
                raise Exception('Job specifications not defined')
            if mountPoints is not None and volumes is None:
                raise Exception("Must provide mounted volume definitions")
        self.jobDefinition: Optional[str] = jobDefinition
        self.jobName: str = jobName if jobName is not None else deployment_name
        self.jobQueue: str = jobQueue
        self.clientKwargs: MutableMapping[str, str] = {}
        if regionName is not None:
            self.clientKwargs['region_name'] = regionName
        if profileName is not None:
            session = boto3.Session(profile_name=profileName)
            self.client = session.client('batch', **self.clientKwargs)
        else:
            # assume necessary information is set via env vars/file
            self.client = boto3.client('batch', **self.clientKwargs)
        self.profileName: Optional[str] = profileName
        self.memory: Optional[int] = memory
        self.mountPoints: Optional[MutableSequence[Any]] = mountPoints
        self.pollingInterval: int = pollingInterval
        self.timeout: Optional[int] = timeout
        self.vcpus: Optional[int] = vcpus
        self.volumes: Optional[MutableSequence[Any]] = volumes
        self.scheduledJobs: MutableSequence[str] = []
        self.jobsCache: TTLCache = TTLCache(maxsize=1, ttl=self.pollingInterval)
        self.commandsCache: MutableMapping[str, MutableMapping[str, str]] = {}

    def _get_run_command(self,
                         command: str,
                         location: str,
                         interactive: bool = False) -> str:
        return (
            "aws "
            "batch "
            "submit-job"
            "--job-name {jobName}"
            "--job-queue {jobQueue} "
            "--job-definition {jobDefinition}"
        ).format(
            jobName=self.jobName,
            jobQueue=self.jobQueue,
            jobDefinition=self.jobDefinition)

    def _get_job_results(self) -> MutableMapping[str, Any]:
        response = self.client.describe_jobs(jobs=self.scheduledJobs)
        job_results: MutableMapping[str, Any] = self.jobsCache.get('job_results', {})
        if 'job_results' in self.jobsCache.keys():
            return job_results
        for job in response['jobs']:
            if job['status'] == 'SUCCEEDED' or job['status'] == 'FAILED':
                if 'exitCode' in job['container']:
                    job_results[job['jobId']] = {
                        'exitCode': job['container']['exitCode'],
                        'logStreamName': job['container']['logStreamName']
                    }
                else:
                    job_results[job['jobId']] = {
                        'exitCode': 1,
                        'reason': job['container']['reason']
                    }
        self.jobsCache['job_results'] = job_results
        return job_results

    def _remove_jobs(self):
        for jobId in self.scheduledJobs:
            self.client.terminate_job(
                jobId=jobId,
                reason='Forced termination'
            )
        self.scheduledJobs = {}

    async def _run(self,
                   location: str,
                   command: MutableSequence[str],
                   environment: MutableMapping[str, str] = None,
                   workdir: Optional[str] = None,
                   stdin: Optional[Union[int, str]] = None,
                   stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                   stderr: Union[int, str] = asyncio.subprocess.STDOUT,
                   job_name: Optional[str] = None,
                   capture_output: bool = False,
                   encode: bool = True,
                   interactive: bool = False,
                   stream: bool = False) -> Union[Optional[Tuple[Optional[Any], int]], asyncio.subprocess.Process]:
        cmd = " ".join(command)
        if job_name is None and location in self.commandsCache:
            if cmd in self.commandsCache[location]:
                return self.commandsCache[location][cmd], 0
        else:
            self.commandsCache[location] = {}
        command = utils.create_command(
            command, environment, workdir, stdin, stdout, stderr)
        logger.debug("Executing command {command} on {location} {job}".format(
            command=command,
            location=location,
            job="for job {job}".format(job=job_name) if job_name else ""))
        jobOverrides: MutableMapping[str, Any] = {}
        if self.attempts is not None:
            jobOverrides['retryStrategy'] = {
                'attempts': self.attempts
            }
        if self.timeout is not None:
            jobOverrides['timeout'] = {
                'attemptDurationSeconds': self.timeout
            }
        jobContainerOverrides = {
            'command': utils.wrap_command(command)
        }
        if self.vcpus is not None:
            jobContainerOverrides['vcpus'] = self.vcpus
        if self.memory is not None:
            jobContainerOverrides['memory'] = self.memory
        if environment is not None:
            jobContainerOverrides['environment'] = [{
                'name': envName,
                'value': envValue
            } for envName, envValue in environment.items()]
        jobOverrides['containerOverrides'] = jobContainerOverrides
        response = self.client.submit_job(
            jobName=self.jobName if self.jobName is not None else job_name,
            jobQueue=self.jobQueue,
            jobDefinition=self.jobDefinition,
            **jobOverrides)
        job_id: str = response['jobId']
        logger.info("Scheduled job {job} with job id {job_id}".format(
            job=job_name,
            job_id=job_id))
        self.scheduledJobs.append(job_id)
        while True:
            job_results = self._get_job_results()
            if job_id in job_results.keys():
                self.scheduledJobs.remove(job_id)
                break
            await asyncio.sleep(self.pollingInterval)
        if 'logStreamName' not in job_results[job_id]:
            return (job_results[job_id]['reason'], job_results[job_id]['exitCode'])
        if self.profileName is not None:
            session = boto3.Session(profile_name=self.profileName)
            cloudwatchLogClient = session.client('logs', **self.clientKwargs)
        else:
            # assume necessary information is set via env vars/file
            cloudwatchLogClient = boto3.client('logs', **self.clientKwargs)
        response = cloudwatchLogClient.get_log_events(
            logGroupName='/aws/batch/job',
            logStreamName=job_results[job_id]['logStreamName'])
        logs: str = ''
        previousToken: str = ''
        while (nextToken := response["nextForwardToken"]) != previousToken:
            if response['events']:
                logs += '\n'.join(map(lambda event: event['message'], response['events'])) + '\n'
            response = cloudwatchLogClient.get_log_events(
                logGroupName='/aws/batch/job',
                logStreamName=job_results[job_id]['logStreamName'],
                nextToken=nextToken)
            previousToken = nextToken
        if logs:
            logs = logs[:-1]
        if job_results[job_id]['exitCode'] == 0:
            self.commandsCache[location][cmd] = logs
        return (logs, job_results[job_id]['exitCode'])

    async def deploy(self, external: bool) -> None:
        if self.jobDefinition is None:
            self.jobDefinition = utils.random_name()
            containerProperties = {
                'image': self.image,
                'vcpus': self.vcpus,
                'memory': self.memory
            }
            if self.mountPoints is not None and self.volumes is not None:
                containerProperties['mountPoints'] = self.mountPoints
                containerProperties['volumes'] = self.volumes
            response = self.client.register_job_definition(
                jobDefinitionName=self.jobDefinition,
                type='container',
                # parameters={
                #     'string': 'string'
                # },
                containerProperties=containerProperties
            )
            logger.info("Created job definition {jobDef}".format(jobDef=self.jobDefinition))

    async def get_available_locations(self,
                                      service: str,
                                      input_directory: str,
                                      output_directory: str,
                                      tmp_directory: str) -> MutableMapping[str, Location]:
        instance_names = [utils.random_name()]
        if self.jobDefinition:
            response = self.client.describe_job_definitions(jobDefinitionName=self.jobDefinition)
            if response['jobDefinitions'] and response['jobDefinitions'][0]['containerProperties']['volumes']:
                instance_names = [volume['efsVolumeConfiguration']['fileSystemId']
                                    for volume in response['jobDefinitions'][0]['containerProperties']['volumes']
                                    if 'efsVolumeConfiguration' in volume]
        return {instance_name: Location(
            name=instance_name,
            hostname='aws',
            slots=1) for instance_name in instance_names}

    async def undeploy(self, external: bool) -> None:
        self._remove_jobs()
        self.scheduledJobs = {}
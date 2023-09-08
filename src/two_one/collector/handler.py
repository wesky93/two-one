import logging
import os
import tempfile

import boto3
import click
import ray
from neomodel import install_all_labels

from two_one.collector.collector import LogFile

logger = logging.getLogger(__name__)

os.environ['RAY_DEDUP_LOGS'] = '0'


def log_obj_list(bucket: str, prefix: str):
    """
    S3 버킷에 저장된 로그 객체를 리스트로 반환합니다.
    :param bucket: S3 버킷 이름
    :param prefix: S3 버킷 내 로그 객체의 경로
    :return: 로그 객체 리스트
    """
    paginator = boto3.client('s3').get_paginator('list_objects_v2')

    resp_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    file_count = 0
    for resp in resp_iterator:
        file_count += resp['KeyCount']
        for obj in resp.get('Contents', []):
            yield obj['Key']
        if resp['IsTruncated']:
            logger.info(f"find {file_count} log files")
            break


@ray.remote
def ingest_log(bucket: str, obj_key: str):
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = os.path.join(temp_dir, obj_key)
            base_dir = os.path.dirname(temp_file)
            os.makedirs(base_dir, exist_ok=True)

            boto3.client('s3').download_file(bucket, obj_key, temp_file)
            file = LogFile(temp_file)
            file.ingest_resource()
    except Exception as e:
        logger.exception(f'fail to ingest {obj_key}')
        return False
    return True


def start_ingest(
        log_bucket: str,
        log_obj_prefix: str,
        max_pending_task: int = 5,
        # todo: 인제스트용 버킷 경로 받기
):
    """
    로그를 인제스트 합니다.
    :param log_bucket: 로그 버킷 이름
    :param log_obj_prefix: 로그 버킷 내 로그 객체 경로
    :param max_pending_task: 한번에 처리할 파일 개수
    :return:
    """
    ray.init()
    install_all_labels()
    refs = dict()
    result_refs = []
    with click.progressbar(log_obj_list(log_bucket, log_obj_prefix)) as keys:

        for log_obj_key in keys:
            if len(result_refs) > max_pending_task:
                _, result_refs = ray.wait(result_refs, num_returns=1)

            ref = ingest_log.remote(log_bucket, log_obj_key)
            refs[log_obj_key] = ref
            result_refs.append(ref)

        # wait until all process finished
        ray.get(result_refs)

    result_mapping = {k: ray.get(ref) for k, ref in refs.items()}

    total_count = len(result_mapping)
    failed_keys = [k for k, v in result_mapping.items() if not v]
    failed_count = len(failed_keys)
    success_count = total_count - failed_count
    logger.info(f'find {total_count} log files')
    logger.info(f'ingested {success_count} log files')
    logger.info(f'failed to ingest {failed_count} log files')

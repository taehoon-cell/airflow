from airflow.sdk import DAG, Variable
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import uuid

from config.settings import Settings
from config.collector_registry import COLLECTORS_CONFIG, get_collector_class


default_args = {
    'owner': 'secops',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
"""
DAG Default Arguments
- owner: DAG 소유자 (secops)
- depends_on_past: 이전 실행 성공 여부 의존 안 함
- email_on_failure: 실패 시 이메일 발송
- retries: 실패 시 3회 재시도
- retry_delay: 재시도 간격 5분
"""


def get_enabled_collectors():
    """
    Airflow Variable에서 활성화된 collector 목록을 조회합니다.

    Airflow Variables:
        enabled_collectors (json): {"ec2": true, "rds": false, ...}

    Returns:
        dict: {collector_name: bool} 형태의 활성화 여부 매핑
              설정값이 없거나 에러 발생 시 모든 Collector를 활성화(True)하여 반환합니다.
    """
    try:
        enabled = Variable.get("enabled_collectors", deserialize_json=True)
        return enabled
    except Exception:
        # 기본값: 모든 collector 활성화
        return {key: True for key in COLLECTORS_CONFIG.keys()}


def check_collector_enabled(collector_name):
    """
    ShortCircuitOperator에서 사용할 Callable을 생성합니다.
    특정 Collector의 활성화 여부를 확인하여 Task 수행 여부를 결정합니다.

    Args:
        collector_name (str): 확인할 Collector 이름 (예: 'ec2')

    Returns:
        function: 활성화 여부(bool)를 반환하는 함수
    """
    def _check(**kwargs):
        enabled_collectors = get_enabled_collectors()
        is_enabled = enabled_collectors.get(collector_name, False)
        print(f"Collector '{collector_name}' enabled: {is_enabled}")
        return is_enabled
    return _check


def create_collect_function(collector_name, config):
    """
    PythonOperator에서 사용할 데이터 수집 함수를 동적으로 생성합니다.

    기능:
    1. Collector 인스턴스 생성
    2. 수집 시작 로깅
    3. data = collector.collect() 실행
    4. collector.send_to_splunk(data) 실행
    5. 수집 완료 로깅

    Args:
        collector_name (str): Collector 이름
        config (dict): Collector 설정 (클래스, Splunk 정보 등)

    Returns:
        function: Task 실행 함수
    """
    def collect_data(**kwargs):
        collection_id = uuid.uuid4()
        conn_id = config.get('splunk_conn_id', 'splunk_default')

        # Collector 클래스 lazy import 후 인스턴스화
        collector_cls = get_collector_class(config)
        collector = collector_cls(
            splunk_conn_id=conn_id
        )

        # 수집 시작 기록
        collector.record_collection_history(
            collector_type=collector_name,
            collection_id=collection_id,
            status='started',
            dag_run_id=kwargs.get('dag_run').run_id if kwargs.get('dag_run') else None
        )

        try:
            # 데이터 수집
            results = collector.collect()

            if results:
                # Splunk로 전송
                splunk_config = Settings.get_splunk_config(conn_id)
                if splunk_config:
                    collector.send_to_splunk(
                        data=results,
                        splunk_conn_id=conn_id,
                        index=config.get('splunk_index', splunk_config.get('index', 'finda-aws')),
                        sourcetype=config.get('splunk_sourcetype')
                    )
                else:
                    print(f"Skipping Splunk export: Connection '{conn_id}' not configured")

            # 수집 완료 기록
            collector.record_collection_history(
                collector_type=collector_name,
                collection_id=collection_id,
                status='completed',
                total_records=len(results) if results else 0,
                dag_run_id=kwargs.get('dag_run').run_id if kwargs.get('dag_run') else None
            )

            # XCom에 결과 요약 반환
            return {
                'collector': collector_name,
                'collection_id': str(collection_id),
                'total_records': len(results) if results else 0
            }

        except Exception as e:
            # 수집 실패 기록
            collector.record_collection_history(
                collector_type=collector_name,
                collection_id=collection_id,
                status='failed',
                error_message=str(e),
                dag_run_id=kwargs.get('dag_run').run_id if kwargs.get('dag_run') else None
            )
            raise

    return collect_data


def create_validate_function(collector_name):
    """
    PythonOperator에서 사용할 검증 함수를 동적으로 생성합니다.

    기능:
    1. 이전 Task(collect_*)의 XCom 결과를 가져옵니다.
    2. 수집된 레코드 수를 확인합니다.
    3. 결과가 없으면 에러를 발생시킵니다.

    Args:
        collector_name (str): Collector 이름

    Returns:
        function: 검증 실행 함수
    """
    def validate_results(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids=f'{collector_name}_group.collect_{collector_name}')

        if not result:
            raise ValueError(f"No collection result found for {collector_name}")

        total_records = result.get('total_records', 0)
        print(f"Validation passed: {total_records} {collector_name} records collected")

        return result

    return validate_results


def aggregate_results(**kwargs):
    """
    모든 Collector의 검증 완료(validate_*)된 결과를 집계합니다.

    Returns:
        dict: 전체 수집 결과 요약
              {'total_collections': int, 'successful': int, 'total_records': int, 'collections': list}
    """
    ti = kwargs['ti']

    results = []
    enabled_collectors = get_enabled_collectors()

    for collector_name in COLLECTORS_CONFIG.keys():
        if enabled_collectors.get(collector_name, False):
            result = ti.xcom_pull(task_ids=f'{collector_name}_group.validate_{collector_name}')
            if result:
                results.append(result)

    summary = {
        'total_collections': len(results),
        'successful': len([r for r in results if r]),
        'total_records': sum(r.get('total_records', 0) for r in results if r),
        'collections': results
    }

    print(f"Aggregation Summary: {summary}")
    return summary


def send_summary_notification(**kwargs):
    """
    집계된 결과를 바탕으로 알림을 전송합니다. (현재는 console print)
    실제 운영 환경에서는 Slack, Email 등으로 연동 가능합니다.
    """
    ti = kwargs['ti']
    summary = ti.xcom_pull(task_ids='aggregate_results')

    if not summary:
        print("No summary data available")
        return

    # 알림 메시지 구성
    message = f"""
    AWS Asset Collection Summary
    ============================
    Total Collections: {summary.get('total_collections', 0)}
    Successful: {summary.get('successful', 0)}
    Total Records Collected: {summary.get('total_records', 0)}

    Details:
    """

    for collection in summary.get('collections', []):
        if collection:
            message += f"\n  - {collection.get('collector', 'unknown')}: {collection.get('total_records', 0)} records"

    print(message)
    return summary


with DAG(
    'aws_resource_collection',
    default_args=default_args,
    description='AWS 자산 수집 통합 DAG (TaskGroup 기반)',
    schedule='0 17 * * *',  # 매일 KST 02:00 (UTC 16:00)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['secops', 'aws', 'inventory', 'master'],
    doc_md="""
    ## AWS Resource Collection DAG

    모든 AWS 리소스 수집을 하나의 DAG에서 관리합니다.

    ### 활성화/비활성화 설정

    Airflow Variable `enabled_collectors`에서 각 collector의 활성화 여부를 설정할 수 있습니다.

    ```json
    {
        "ec2": true,
        "rds": true,
        "dynamodb": true,
        "ebs": true,
        "s3": true,
        "security_group": true,
        "ecr": true,
        "config": true
    }
    ```

    ### Collectors
    - ec2: EC2 Instance 정보 수집
    - rds: RDS Instance 정보 수집
    - dynamodb: DynamoDB Table 정보 수집
    - ebs: 미사용 EBS Volume 수집
    - s3: S3 Bucket 정보 수집
    - security_group: Security Group 수집
    - ecr: ECR Repository 수집
    - config: AWS Config 리소스 통계 수집
    """,
) as dag:

    start = EmptyOperator(task_id='start')

    # 각 collector별 TaskGroup 생성
    task_groups = {}

    for collector_name, config in COLLECTORS_CONFIG.items():
        with TaskGroup(group_id=f'{collector_name}_group') as tg:
            # 활성화 여부 체크
            check_enabled = ShortCircuitOperator(
                task_id=f'check_{collector_name}_enabled',
                python_callable=check_collector_enabled(collector_name),
            )

            # 수집 태스크
            collect_task = PythonOperator(
                task_id=f'collect_{collector_name}',
                python_callable=create_collect_function(collector_name, config),
            )

            # 검증 태스크
            validate_task = PythonOperator(
                task_id=f'validate_{collector_name}',
                python_callable=create_validate_function(collector_name),
            )

            check_enabled >> collect_task >> validate_task

        task_groups[collector_name] = tg

    # 결과 집계
    aggregate_task = PythonOperator(
        task_id='aggregate_results',
        python_callable=aggregate_results,
        trigger_rule='none_failed',  # 일부 collector가 skip되어도 실행
    )

    # 알림 전송
    notify_task = PythonOperator(
        task_id='send_summary_notification',
        python_callable=send_summary_notification,
    )

    end = EmptyOperator(task_id='end')

    # Task 의존성 설정: 모든 TaskGroup이 병렬 실행 후 집계
    start >> list(task_groups.values()) >> aggregate_task >> notify_task >> end

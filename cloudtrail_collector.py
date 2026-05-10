"""
AWS CloudTrail Collector

다중 계정 환경에서 CloudTrail 설정 정보를 수집합니다.

수집 항목:
- Trail 기본 정보 (Name, Arn, HomeRegion)
- 로깅 설정 상태 (IsLogging, LatestDeliveryTime)
- S3 버킷 및 SNS 설정
- Multi-Region 여부, Log Validation 여부, KMS Key ID
"""

from typing import List, Dict, Any, Optional
from collectors.base_collector import BaseCollector
from botocore.exceptions import ClientError
import logging
from typing import Any as TypingAny

logger = logging.getLogger(__name__)


class CloudTrailCollector(BaseCollector):
    """AWS CloudTrail Collector - 다중 계정 CloudTrail 설정 정보 수집"""

    def collect(self) -> List[Dict[str, Any]]:
        """
        모든 대상 계정의 CloudTrail 정보를 수집합니다.

        Returns:
            list: 수집된 CloudTrail 정보 딕셔너리 리스트
        """
        accounts = self.get_all_accounts()
        all_results: List[Dict[str, Any]] = []

        for account in accounts:
            account_id = account['account_id']
            account_name = account.get('account_name', account_id)

            try:
                # CloudTrail은 Region별 리소스지만, describe_trails(includeShadowTrails=True)를 쓰거나
                # 각 리전을 순회해야 함. 여기서는 Home Region에서만 수집하거나, 주요 리전을 순회하는 방식 선택.
                # 보통 CloudTrail 설정은 관리 계정이나 특정 리전에서 통합 관리되기도 하지만,
                # 개별 Trail은 각 리전 API로 조회해야 정확함.
                # 그러나 describe_trails는 해당 리전의 Trail만 보여줌.
                # Multi-region trail은 Home region에서 조회하는 것이 가장 정확한 설정을 볼 수 있음.
                # 효율성을 위해 각 계정의 'ap-northeast-2' (Main Region) 을 먼저 조회하고,
                # 필요시 다른 리전도 순회할 수 있으나, 일단 Main AWS Region (ap-northeast-2) 우선 수집.
                
                # 하지만 CloudTrail은 글로벌 서비스 성격이 있어 us-east-1 등에서도 확인 필요할 수 있음.
                # 여기서는 BaseCollector의 self.region (ap-northeast-2)을 사용하여 수집.
                
                results = self._collect_from_account(account_id, account_name)
                all_results.extend(results)
                print(f"Collected {len(results)} CloudTrails from {account_name}")
            except Exception as e:
                print(f"Error collecting CloudTrail from account {account_name}: {e}")
                logger.error(f"Error collecting CloudTrail from account {account_name}: {e}")

        return all_results

    def _collect_from_account(self, account_id: str, account_name: str) -> List[Dict[str, Any]]:
        """
        특정 계정에서 CloudTrail 정보 수집
        """
        # CloudTrail 클라이언트 생성 (기본 리전)
        ct_client = self.get_client_for_account(account_id, 'cloudtrail')
        results: List[Dict[str, Any]] = []

        try:
            # Shadow Trails 포함 여부는 선택적이나, 실제 구성된 Trail만 보려면 False가 나을 수 있음.
            # 하지만 Multi-region trail의 경우 타 리전에서는 Shadow로 보일 수 있음.
            # 여기서는 includeShadowTrails=False 로 Home Region에 생성된 것만 수집.
            response = ct_client.describe_trails(includeShadowTrails=False)
            
            for trail in response['trailList']:
                trail_arn = trail.get('TrailARN')
                
                # 로깅 상태 조회 (get_trail_status)
                status_info = {}
                try:
                    status_resp = ct_client.get_trail_status(Name=trail_arn)
                    status_info = {
                        'is_logging': status_resp.get('IsLogging'),
                        'latest_delivery_time': status_resp.get('LatestDeliveryTime'),
                        'latest_delivery_error': status_resp.get('LatestDeliveryError'),
                        'start_logging_time': status_resp.get('StartLoggingTime'),
                        'stop_logging_time': status_resp.get('StopLoggingTime')
                    }
                except ClientError as e:
                    logger.warning(f"Error getting trail status for {trail_arn}: {e}")

                # Tags 수집
                tags = self._get_trail_tags(ct_client, trail_arn)

                trail_data = {
                    'account_id': account_id,
                    'account_name': account_name,
                    'region': trail.get('HomeRegion', self.region),
                    'name': trail.get('Name'),
                    'arn': trail_arn,
                    'is_multi_region_trail': trail.get('IsMultiRegionTrail', False),
                    'home_region': trail.get('HomeRegion'),
                    's3_bucket_name': trail.get('S3BucketName'),
                    's3_key_prefix': trail.get('S3KeyPrefix'),
                    'sns_topic_arn': trail.get('SnsTopicARN'), # SNS Topic Name or ARN
                    'include_global_service_events': trail.get('IncludeGlobalServiceEvents', False),
                    'is_organization_trail': trail.get('IsOrganizationTrail', False),
                    'log_file_validation_enabled': trail.get('LogFileValidationEnabled', False),
                    'kms_key_id': trail.get('KmsKeyId'),
                    'has_custom_event_selectors': trail.get('HasCustomEventSelectors', False),
                    'cloud_watch_logs_log_group_arn': trail.get('CloudWatchLogsLogGroupArn'),
                    'cloud_watch_logs_role_arn': trail.get('CloudWatchLogsRoleArn'),
                    # Status Info
                    'is_logging': status_info.get('is_logging'),
                    'latest_delivery_time': status_info.get('latest_delivery_time').isoformat() if status_info.get('latest_delivery_time') else None,
                    'latest_delivery_error': status_info.get('latest_delivery_error'),
                    'tags': tags
                }
                results.append(trail_data)

        except ClientError as e:
            logger.error(f"Error describing trails for account {account_id}: {e}")
            raise

        return results

    def _get_trail_tags(self, ct_client: TypingAny, trail_arn: str) -> Dict[str, str]:
        """Trail 태그 수집"""
        try:
            response = ct_client.list_tags(ResourceIdList=[trail_arn])
            resource_tag_list = response.get('ResourceTagList', [])
            if resource_tag_list:
                return {tag['Key']: tag['Value'] for tag in resource_tag_list[0].get('TagsList', [])}
            return {}
        except ClientError:
            return {}

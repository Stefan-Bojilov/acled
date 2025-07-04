import json
import os
from typing import Literal

import boto3
import boto3.session
from botocore.exceptions import BotoCoreError, ClientError


class SecretManager:
    def __init__(self,region_name: str| None):
        """
        secret_name: Name of the secret in AWS Secrets Manager or environment
        region_name: AWS region, defaults to AWS_REGION env var or 'us-east-1'
        """
        self.region_name = region_name or os.getenv("AWS_REGION", "eu-north-1")

        session = boto3.session.Session()
        self.client = session.client(
            service_name="secretsmanager",
            region_name=self.region_name
        )

    def get_secret(self, secret_name: str, source: Literal["aws", "env"] = "aws") -> str | None:
        if source == "env":
            return os.getenv(secret_name)

        try:
            response = self.client.get_secret_value(SecretId=secret_name)
            secret_str = response.get("SecretString")
            if secret_str:
                secret_dict = json.loads(secret_str)
                return secret_dict.get(secret_name)  
            return None
        except (ClientError, BotoCoreError, json.JSONDecodeError) as e:
            print(f"[SecretManager] Error fetching '{secret_name}': {e}")
            return None
            
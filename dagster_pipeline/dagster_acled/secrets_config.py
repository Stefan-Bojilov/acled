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

    def get_secret(
        self,
        secret_name: str,
        source: Literal["aws", "env"] = "aws",
        field_name: str | None = None,
    ):
        """
        Fetches a secret by name.  
        - source="env": loads from environment variable.  
        - source="aws": loads from AWS Secrets Manager.
        If `field_name` is provided and the secret is JSON, returns that field’s value.
        Otherwise returns the full JSON dict or raw string.
        """
        # Fetch from environment
        if source == "env":
            value = os.getenv(secret_name)
            if not value:
                return None
            try:
                parsed = json.loads(value)
                if field_name:
                    return parsed.get(field_name)
                return parsed
            except json.JSONDecodeError:
                return value

        try:
            response = self.client.get_secret_value(SecretId=secret_name)
            secret_str = response.get("SecretString")
            if secret_str:
                try:
                    secret_dict = json.loads(secret_str)
                    if field_name:
                        return secret_dict.get(field_name)
                    return secret_dict
                except json.JSONDecodeError:
                    # Not JSON—return raw string
                    return secret_str
            return None
        except (ClientError, BotoCoreError) as e:
            print(f"[SecretManager] Error fetching '{secret_name}': {e}")
            return None

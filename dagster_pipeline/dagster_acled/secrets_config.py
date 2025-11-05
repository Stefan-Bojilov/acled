import json
import os
from typing import Literal
from pathlib import Path

import boto3
import boto3.session
from botocore.exceptions import BotoCoreError, ClientError

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient




class SecretManager:
    def __init__(self,
                 vault_url: str | None = None,
                 region_name: str| None = None):
        """
        secret_name: Name of the secret in AWS Secrets Manager or environment
        region_name: AWS region, defaults to AWS_REGION env var or 'us-east-1'
        """
        self.vault_url = vault_url or os.getenv("AZURE_VAULT_URL")

        self.region_name = region_name or os.getenv("AWS_REGION", "eu-north-1")
        session = boto3.session.Session()
        self.aws_client = session.client(
            service_name="secretsmanager",
            region_name=self.region_name
        )

        self.azure_client = None
        if self.vault_url:
            credential = DefaultAzureCredential()
            self.azure_client = SecretClient(vault_url=self.vault_url, credential=credential)

    def get_secret(
        self,
        secret_name: str,
        source: Literal["aws", "az", "env"] = "az",
        field_name: str | None = None,
    ):
        """
        Fetches a secret by name.  
        - source="env": loads from environment variable.  
        - source="aws": loads from AWS Secrets Manager.
        If `field_name` is provided and the secret is JSON, returns that field’s value.
        Otherwise returns the full JSON dict or raw string.
        """
        if source == "env":
            value = os.getenv(secret_name)
            if not value:
                return None
            try:
                parsed = json.loads(value)
                return parsed.get(field_name) if field_name else parsed
            except json.JSONDecodeError:
                return value

        if source == "aws":
            try:
                response = self.aws_client.get_secret_value(SecretId=secret_name)
                secret_str = response.get("SecretString")
                if secret_str:
                    try:
                        secret_dict = json.loads(secret_str)
                        return secret_dict.get(field_name) if field_name else secret_dict
                    except json.JSONDecodeError:
                        return secret_str
                return None
            except (ClientError, BotoCoreError) as e:
                print(f"[SecretManager] AWS error: {e}")
                return None

        if source == "az":
            if not self.azure_client:
                print("[SecretManager] Azure client not initialized")
                return None
            try:
                secret = self.azure_client.get_secret(secret_name)
                secret_value = secret.value
                if secret_value:
                    try:
                        secret_dict = json.loads(secret_value)
                        return secret_dict.get(field_name) if field_name else secret_dict
                    except json.JSONDecodeError:
                        return secret_value
                return None
            except Exception as e:
                print(f"[SecretManager] Azure error: {e}")
                return None

        return None
        
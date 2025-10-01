import os  # noqa: I001
import aiohttp
from typing import Optional, Union
from dotenv import load_dotenv
from datetime import datetime, timedelta

from dagster_acled.secrets_config import SecretManager
from dagster import Config
from pydantic import Field, BaseModel, field_serializer, field_validator, ConfigDict, computed_field
from typing import Any

load_dotenv()

class TokenData(BaseModel):
    """Model for OAuth token response data."""
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "Bearer"
    expires_in: int = Field(default=86400, description="Token expiration time in seconds")
    expires_at: Optional[datetime] = Field(default=None, description="Calculated expiration timestamp")
    
    @field_serializer('expires_at')
    def serialize_expires_at(self, expires_at: Optional[datetime]) -> Optional[str]:
        """Serialize expires_at datetime to ISO format string."""
        if expires_at is None:
            return None
        return expires_at.isoformat()
    
    def __init__(self, **data):
        """Initialize and immediately calculate expires_at from expires_in."""
        super().__init__(**data)
        
        # Calculate expires_at immediately when token data is created
        if not self.expires_at and self.expires_in:
            self.expires_at = datetime.now() + timedelta(seconds=self.expires_in)
    
    @property
    def is_expired(self) -> bool:
        """Check if the token is expired (with 5-minute buffer)."""
        if not self.expires_at:
            return True
        
        buffer_time = datetime.now() + timedelta(minutes=5)
        return buffer_time >= self.expires_at
    
    @property
    def time_until_expiration(self) -> Optional[timedelta]:
        """Get time remaining until token expires."""
        if not self.expires_at:
            return None
        return self.expires_at - datetime.now()


class OAuthTokenManager(BaseModel):
    """Manages OAuth tokens for ACLED API authentication using Pydantic."""
    
    username: str | None = Field(default=None, description="Username (email) for OAuth authentication")
    password: str | None = Field(default=None, description="Password for OAuth authentication")
    base_url: str = Field(default="https://acleddata.com", description="Base URL for OAuth endpoints")
    
    # Token storage - store the complete token data
    current_token: TokenData | None = Field(default=None, description="Current token data")
    
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )
    
    def __init__(self, **data):
        """Initialize and load credentials if needed."""
        super().__init__(**data)
        
        # Load credentials from secrets if not provided
        if not self.username or not self.password:
            self._load_credentials_from_secrets()
            
        if not self.username or not self.password:
            raise ValueError("Username and password are required for OAuth authentication")
    
    @field_validator('base_url')
    @classmethod
    def strip_trailing_slash(cls, v: str) -> str:
        """Remove trailing slash from base URL."""
        return v.rstrip('/')
    
    @computed_field
    @property
    def token_url(self) -> str:
        """OAuth token endpoint URL."""
        return f"{self.base_url.rstrip('/')}/oauth/token"
    
    def _load_credentials_from_secrets(self) -> None:
        """Load credentials from AWS Secrets Manager."""
        if not self.username or not self.password:
            sm = SecretManager(region_name=os.environ['REGION_NAME'])
            credentials = sm.get_secret('ACLED-API')
        if credentials:
            self.username = credentials.get('username')
            self.password = credentials.get('password')

    def __repr__(self) -> str:
        """Safe representation that doesn't expose password."""
        has_token = self.current_token is not None
        is_valid = self.is_token_valid if has_token else False
        return f"OAuthTokenManager(username='{self.username}', base_url='{self.base_url}', has_token={has_token}, is_valid={is_valid})"
        
    @property
    def access_token(self) -> Optional[str]:
        """Get the current access token."""
        return self.current_token.access_token if self.current_token else None
    
    @property
    def refresh_token(self) -> Optional[str]:
        """Get the current refresh token."""
        return self.current_token.refresh_token if self.current_token else None
    
    @property
    def token_expires_at(self) -> Optional[datetime]:
        """Get the token expiration time."""
        return self.current_token.expires_at if self.current_token else None
    
    @property
    def is_token_valid(self) -> bool:
        """Check if the current token is valid (exists and not expired)."""
        if not self.current_token:
            return False
        return not self.current_token.is_expired
    
    @property
    def time_until_expiration(self) -> Optional[timedelta]:
        """Get time remaining until token expires."""
        if not self.current_token:
            return None
        return self.current_token.time_until_expiration
    
    async def get_access_token(self) -> str:
        """
        Get a valid access token, refreshing if necessary.
        
        Returns:
            Valid access token string
            
        Raises:
            Exception: If unable to obtain or refresh token
        """
        if self.is_token_valid:
            return self.access_token
        
        # Try to refresh token if we have a refresh token
        if self.refresh_token:
            try:
                await self._refresh_access_token()
                if self.is_token_valid:
                    return self.access_token
            except Exception as e:
                # If refresh fails, we'll get a new token below
                pass
        
        # Get new token
        await self._get_new_token()
        
        if not self.access_token:
            raise Exception("Failed to obtain access token")
            
        return self.access_token
    
    async def _get_new_token(self) -> None:
        """Request a new access token using username/password."""
        # Use multipart/form-data as specified in the API docs
        data = aiohttp.FormData()
        data.add_field('username', self.username)
        data.add_field('password', self.password)
        data.add_field('grant_type', 'password')
        data.add_field('client_id', 'acled')
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.token_url,
                data=data
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(
                        f"Failed to get access token: {response.status} - {error_text}"
                    )
                
                token_data = await response.json()
                # TokenData will automatically calculate expires_at when created
                self.current_token = TokenData(**token_data)
    
    async def _refresh_access_token(self) -> None:
        """Refresh the access token using the refresh token."""
        if not self.refresh_token:
            raise Exception("No refresh token available")
        
        # Use multipart/form-data for refresh as well
        data = aiohttp.FormData()
        data.add_field('refresh_token', self.refresh_token)
        data.add_field('grant_type', 'refresh_token')
        data.add_field('client_id', 'acled')
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.token_url,
                data=data
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(
                        f"Failed to refresh token: {response.status} - {error_text}"
                    )
                
                token_data = await response.json()
                # Create new TokenData, preserving refresh token if not provided in response
                new_token_data = TokenData(**token_data)
                
                # If refresh response doesn't include a new refresh token, keep the old one
                if not new_token_data.refresh_token and self.refresh_token:
                    new_token_data.refresh_token = self.refresh_token
                
                self.current_token = new_token_data
    
    def get_auth_header(self) -> dict[str, str]:
        """
        Get the authorization header for API requests.
        
        Returns:
            Dictionary with Authorization header
            
        Raises:
            Exception: If no valid token is available
        """
        if not self.access_token:
            raise Exception("No access token available. Call get_access_token() first.")
        
        return {'Authorization': f'Bearer {self.access_token}'}
    
    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary, excluding sensitive information.
        
        Returns:
            Dictionary representation without password
        """
        token_info = {}
        if self.current_token:
            token_info = {
                'has_access_token': True,
                'has_refresh_token': self.current_token.refresh_token is not None,
                'token_type': self.current_token.token_type,
                'expires_at': self.current_token.expires_at.isoformat() if self.current_token.expires_at else None,
                'is_expired': self.current_token.is_expired,
                'time_until_expiration': str(self.current_token.time_until_expiration) if self.current_token.time_until_expiration else None
            }
        else:
            token_info = {
                'has_access_token': False,
                'has_refresh_token': False,
                'token_type': None,
                'expires_at': None,
                'is_expired': True,
                'time_until_expiration': None
            }
        
        return {
            'username': self.username,
            'base_url': self.base_url,
            'token_url': self.token_url,
            **token_info
        }


class AcledClientConfig(Config):
    """
    Configuration for the ACLED client with OAuth authentication.
    """

    base_url: str = Field(
        default="https://acleddata.com/api/",
        description="The base URL for the ACLED API."
    )
    
    oauth_base_url: str = Field(
        default="https://acleddata.com",
        description="The base URL for OAuth authentication."
    )
    
    endpoint: str = Field(
        default="acled/read",
        description="The specific endpoint for the ACLED API."
    )
    
    username: str = Field(
        default='',
        description="Username (email) for ACLED OAuth authentication."
    )
    
    password: str = Field(
        default='',  # Will be loaded from secrets
        description="Password for ACLED OAuth authentication."
    )

    max_pages: int = Field(
        default=5000, 
        description="Maximum number of pages to fetch from the ACLED API."
    )
    
    @field_validator('oauth_base_url', 'base_url')
    @classmethod
    def strip_trailing_slash(cls, v: str) -> str:
        """Remove trailing slash from URLs."""
        return v.rstrip('/')
    
    def get_oauth_manager(self) -> OAuthTokenManager:
        """Get OAuth manager with credentials from this config."""
        return OAuthTokenManager(
            username=self.username,  # Let OAuthTokenManager handle empty strings
            password=self.password,
            base_url=self.oauth_base_url
        )

class AcledConfig(AcledClientConfig):
    """
    Configuration for the ACLED data pipeline.
    """

    event_id_cnty: Optional[str] = Field(default=None, description="Filter on event_id_cnty (LIKE)")
    event_id_cnty_where: Optional[str] = Field(default=None, description="Override operator for event_id_cnty filter")

    event_date: Optional[int] = Field(default=None, description="Filter on event_date (equals)")
    event_date_where: Optional[str] = Field(default=None, description="Override operator for event_date filter")

    year: Optional[int] = Field(default=None, description="Filter on year (equals)")
    year_where: Optional[str] = Field(default=None, description="Override operator for year filter")

    time_precision: Optional[int] = Field(default=None, description="Filter on time_precision (equals)")
    time_precision_where: Optional[str] = Field(default=None, description="Override operator for time_precision filter")

    disorder_type: Optional[str] = Field(default=None, description="Filter on disorder_type (LIKE)")
    disorder_type_where: Optional[str] = Field(default=None, description="Override operator for disorder_type filter")

    event_type: Optional[str] = Field(default=None, description="Filter on event_type (LIKE)")
    event_type_where: Optional[str] = Field(default=None, description="Override operator for event_type filter")

    sub_event_type: Optional[str] = Field(default=None, description="Filter on sub_event_type (LIKE)")
    sub_event_type_where: Optional[str] = Field(default=None, description="Override operator for sub_event_type filter")

    actor1: Optional[str] = Field(default=None, description="Filter on actor1 (LIKE)")
    actor1_where: Optional[str] = Field(default=None, description="Override operator for actor1 filter")

    assoc_actor_1: Optional[str] = Field(default=None, description="Filter on assoc_actor_1 (LIKE)")
    assoc_actor_1_where: Optional[str] = Field(default=None, description="Override operator for assoc_actor_1 filter")

    inter1: Optional[int] = Field(default=None, description="Filter on inter1 (equals)")
    inter1_where: Optional[str] = Field(default=None, description="Override operator for inter1 filter")

    actor2: Optional[str] = Field(default=None, description="Filter on actor2 (LIKE)")
    actor2_where: Optional[str] = Field(default=None, description="Override operator for actor2 filter")

    assoc_actor_2: Optional[str] = Field(default=None, description="Filter on assoc_actor_2 (LIKE)")
    assoc_actor_2_where: Optional[str] = Field(default=None, description="Override operator for assoc_actor_2 filter")

    inter2: Optional[int] = Field(default=None, description="Filter on inter2 (equals)")
    inter2_where: Optional[str] = Field(default=None, description="Override operator for inter2 filter")

    interaction: Optional[int] = Field(default=None, description="Filter on interaction (equals)")
    interaction_where: Optional[str] = Field(default=None, description="Override operator for interaction filter")

    civilian_targeting: Optional[str] = Field(default=None, description="Filter on civilian_targeting (LIKE)")
    civilian_targeting_where: Optional[str] = Field(default=None, description="Override operator for civilian_targeting filter")

    iso: Optional[str] = Field(default="804", description="Filter on iso (equals)")
    iso_where: Optional[str] = Field(default=None, description="Override operator for iso filter")

    region: Optional[int] = Field(default=None, description="Filter on region (equals)")
    region_where: Optional[str] = Field(default=None, description="Override operator for region filter")

    country: Optional[str] = Field(default=None, description="Filter on country (equals)")
    country_where: Optional[str] = Field(default=None, description="Override operator for country filter")

    admin1: Optional[str] = Field(default=None, description="Filter on admin1 (LIKE)")
    admin1_where: Optional[str] = Field(default=None, description="Override operator for admin1 filter")

    admin2: Optional[str] = Field(default=None, description="Filter on admin2 (LIKE)")
    admin2_where: Optional[str] = Field(default=None, description="Override operator for admin2 filter")

    admin3: Optional[str] = Field(default=None, description="Filter on admin3 (LIKE)")
    admin3_where: Optional[str] = Field(default=None, description="Override operator for admin3 filter")

    location: Optional[str] = Field(default=None, description="Filter on location (LIKE)")
    location_where: Optional[str] = Field(default=None, description="Override operator for location filter")

    latitude: Optional[float] = Field(default=None, description="Filter on latitude (equals)")
    latitude_where: Optional[str] = Field(default=None, description="Override operator for latitude filter")

    longitude: Optional[float] = Field(default=None, description="Filter on longitude (equals)")
    longitude_where: Optional[str] = Field(default=None, description="Override operator for longitude filter")

    geo_precision: Optional[int] = Field(default=None, description="Filter on geo_precision (equals)")
    geo_precision_where: Optional[str] = Field(default=None, description="Override operator for geo_precision filter")

    source: Optional[str] = Field(default=None, description="Filter on source (LIKE)")
    source_where: Optional[str] = Field(default=None, description="Override operator for source filter")

    source_scale: Optional[str] = Field(default=None, description="Filter on source_scale (LIKE)")
    source_scale_where: Optional[str] = Field(default=None, description="Override operator for source_scale filter")

    notes: Optional[str] = Field(default=None, description="Filter on notes (LIKE)")
    notes_where: Optional[str] = Field(default=None, description="Override operator for notes filter")

    fatalities: Optional[int] = Field(default=None, description="Filter on fatalities (equals)")
    fatalities_where: Optional[str] = Field(default=None, description="Override operator for fatalities filter")

    timestamp: Optional[Union[int]] = Field(default=None, description="Filter on timestamp (>=)")
    timestamp_where: Optional[str] = Field(default=None, description="Override operator for timestamp filter")

    export_type: Optional[str] = Field(default=None, description="Filter on export_type (equals)")
    export_type_where: Optional[str] = Field(default=None, description="Override operator for export_type filter")

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    
    async def build_params(self) -> tuple[dict, dict]: 
        """
        Build query parameters using model_dump and headers with OAuth authentication.
    
        Returns:
        tuple: (params dict, headers dict with Authorization)
        """
        
        oauth_manager = self.get_oauth_manager()
        access_token = await oauth_manager.get_access_token()
        headers = {'Authorization': f'Bearer {access_token}'}

        model_data = self.model_dump(exclude_none=True, exclude=['username', 'password', 'oauth_base_url', 
                                                                 'base_url', 'endpoint', 'max_pages'])
        
        params = {}
        for field, value in model_data.items():
            if field.endswith("_where"):
                continue
                
            # Check if there's a corresponding _where field
            where_field = f"{field}_where"
            if where_field in model_data:
                key = where_field
            else:
                key = field
                
            params[key] = value

        return params, headers


    @property
    def full_url(self) -> str:
        """
        Returns the fully formatted API request URL including filters.
        """
        from urllib.parse import urlencode
        base = f"{self.base_url.rstrip('/')}/{self.endpoint}"
        query = urlencode(self.build_params(), doseq=True)
        return f"{base}?{query}"

import os
from typing import Optional, Union

from config.secrets_config import SecretManager
from dagster import Config
from pydantic import Field

sm = SecretManager(region_name="eu-north-1")
API_KEY = sm.get_secret('ACLED-API')

class AcledClientConfig(Config):
    """
    Configuration for the ACLED client.
    """

    base_url: str = Field(
        default="https://api.acleddata.com/",
        description="The endpoint for the ACLED API."
    )
    api_key: str = Field(
        default=API_KEY,
        description="API key for accessing the ACLED API."
    )
    endpoint: str = Field(
        default="acled/read",
        description="The specific endpoint for the ACLED API."
    )
    email: str = Field(
        default=os.environ['EMAIL'],
        description="Email associated with the ACLED API account."
    )

    max_pages: int = Field(
        default=5000, 
        description="Maximum number of pages to fetch from the ACLED API."
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

    iso: Optional[int] = Field(default=804, description="Filter on iso (equals)")
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

    def build_params(self) -> dict:
        """
        Assemble query parameters including API key, email, and any filters.
        """
        params = {"key": self.api_key, "email": self.email}
        for field, value in self.__dict__.items():
            if field in {"api_key", "email"} or field.endswith("_where"):
                continue

            if value is None:
                continue

            where = getattr(self, f"{field}_where", None)
            key = f"{field}_where" if where else field
            params[key] = value

        return params

    @property
    def full_url(self) -> str:
        """
        Returns the fully formatted API request URL including filters.
        """
        from urllib.parse import urlencode
        base = f"{self.base_url.rstrip('/')}/{self.endpoint}"
        query = urlencode(self.build_params(), doseq=True)
        return f"{base}?{query}"

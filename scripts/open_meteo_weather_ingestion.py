from openmeteo_requests import Client
from requests_cache import CachedSession
from retry_requests import retry
import hashlib

import json
from typing import Dict, Any, Optional, Callable
from utils import make_json_serializable, now_dt
import pandas as pd

class OpenMeteoWeatherIngestion:
    """a
    A class to extract weather data from the Open-Meteo API and store it in Databricks workspace.
    
    This class handles both current weather conditions and hourly forecasts, with built-in
    caching and retry logic for API requests.
    """

    def __init__(self,username:str,dbutils) -> None:
        """
        Initialize the WeatherDataExtractor with API client configuration.
        
        Sets up the Open-Meteo API client with caching (1 hour expiry) and retry mechanism
        (5 retries with exponential backoff).
        """
        # Setup the Open-Meteo API client with cache and retry on error
        self.username = username
        self.dbutils = dbutils
        cache_session: CachedSession = CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo: Client = Client(session=retry_session)
        
        # Default parameters for all weather requests
        self.params: Dict[str, Any] = {
            "wind_speed_unit": "kn",
            "timezone": "auto",
        }
    
    def get_times(self,hourly):
        return pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s"),
            end =  pd.to_datetime(hourly.TimeEnd(), unit = "s"),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        ).to_numpy()
    
    def __write_data_to_folder(self, data: Dict[str, Any], sub_folder: str = 'current') -> None:
        """
        Write weather data to a JSON file in the user's workspace folder.
        
        Args:
            data: Dictionary containing weather data to be saved
            sub_folder: Subfolder name within the weather data directory (default: 'current')
        
        Note:
            Files are named using epoch timestamp and SHA256 hash to ensure uniqueness.
        """
        # Get current Databricks username

        # Add query timestamp to data
        
        # Construct folder path in user's workspace
        folder: str = f'/Workspace/Users/{self.username}/openmeteo-databricks-pipeline/data/forecast/{sub_folder}/'

        # Generate unique filename using epoch timestamp and hash
        hasher = hashlib.sha256()
        epoch_seconds: int = int(now_dt().timestamp())
        file_name: str = f'{epoch_seconds}_{hasher.hexdigest()}.json'

        print(file_name)
        
        self.dbutils.fs.put(folder + file_name, json.dumps(data), True)

    def __current_config(self) -> None:
        """
        Configure parameters for current weather conditions request.
        
        Updates the params dictionary with current weather variables including
        temperature, humidity, precipitation, wind metrics, and weather codes.
        """
        self.params.update({
            "current": [
                "temperature_2m", 
                "relative_humidity_2m", 
                "is_day", 
                "precipitation", 
                "rain", 
                "showers",
                "weather_code", 
                "wind_speed_10m", 
                "wind_direction_10m", 
                "wind_gusts_10m"
            ]
        })

    def __forecast_16_hours_config(self) -> None:
        """
        Configure parameters for 16-day hourly forecast request.
        
        Updates the params dictionary with hourly forecast variables including
        temperature, humidity, precipitation, pressure, and wind metrics at
        multiple altitude levels (10m, 80m, 120m, 180m).
        """
        self.params.update({
            "hourly": [
                "temperature_2m", 
                "relative_humidity_2m", 
                "precipitation",
                "showers",
                "weather_code", 
                "pressure_msl", 
                "wind_speed_10m", 
                "wind_speed_80m", 
                "wind_speed_120m", 
                "wind_speed_180m", 
                "wind_direction_10m", 
                "wind_direction_80m", 
                "wind_direction_120m", 
                "wind_direction_180m", 
                "wind_gusts_10m", 
                "temperature_80m", 
                "temperature_120m", 
                "temperature_180m", 
                "rain"
            ],
            "forecast_days": 16,
        })

    def __process_weather_section(
        self, 
        response_section: Any, 
        base_results: Dict[str, Any], 
        variables_key: str, 
        output_folder:str
    ) -> dict:
        """
        Process a section of the weather API response and save it to disk.
        
        Args:
            response_section: API response section object (Current or Hourly)
            base_results: Base dictionary containing latitude, longitude, and elevation
            variables_key: Key to access variable names in params ('current' or 'hourly')
            output_folder: Target folder name for saving the data
        """
        # Create a copy of base results to avoid mutation
        section_results: Dict[str, Any] = {}
        section_results["timestamp"] = str(response_section.Time())
        variable_extraction = lambda x:  response_section.Variables(x)
        # Extract all variable values from the response
        section_results.update({
            variable_name: make_json_serializable(variable_extraction(index).Value()) if variables_key.lower() == 'current' else make_json_serializable(variable_extraction(index).ValuesAsNumpy())
            for index, variable_name in enumerate(self.params[variables_key])
        })

        if variables_key.lower() == 'hourly':
            section_results.update({ 'timestamp' :make_json_serializable(self.get_times(response_section))})
            section_results = [
                 {**base_results,**dict(zip(section_results.keys(), values)) }
                for values in zip(*section_results.values())
            ]
        
        else:
            section_results.update(base_results)


        
            
            
        self.__write_data_to_folder(section_results, output_folder)

    def get_weather(
        self, 
        latitude: float | list[float], 
        longitude: float | list[float], 
        mode: str = 'current' 
    ) -> None:
        """
        Fetch weather data for specified coordinates and save to workspace.
        
        Args:
            latitude: Latitude coordinate for weather location
            longitude: Longitude coordinate for weather location
            include_current: Whether to fetch current weather conditions (default: True)
            include_forecast: Whether to fetch 16-day hourly forecast (default: True)
        
        Note:
            Data is automatically saved to the user's workspace in JSON format.
            Current weather is saved to 'current/' subfolder.
            Forecast data is saved to 'hourly/' subfolder.
        """
        url: str = "https://api.open-meteo.com/v1/forecast"

        # Set location parameters
        self.params["latitude"] = latitude
        self.params["longitude"] = longitude

        # Apply configuration based on requested data types
        config_options: list[tuple[bool, Callable[[], None]]] = [
            (mode=='current', self.__current_config),
            (mode!='current', self.__forecast_16_hours_config)
        ]
        
        for enabled, config_function in config_options:
            if enabled:
                config_function()

        # Make API request
        responses = self.openmeteo.weather_api(url, params=self.params)

        def get_results(response):

            # Prepare base results with location metadata
            base_results: Dict[str, Any] = {
                "latitude": response.Latitude(),
                "longitude": response.Longitude(),
                "elevation": response.Elevation(),
                'timezone': str(response.Timezone().decode('utf-8')),
                'timezone_abbreviation': str(response.TimezoneAbbreviation().decode('utf-8')),
                'query_timestamp': str(now_dt())}

            sections: list[tuple[bool, Any, str, str]] = [
                (mode=='current', response.Current, "current", "current"),
                (mode!='current', response.Hourly, "hourly", "hourly")
            ]

            for enabled, response_getter, variables_key, output_folder in sections:
                if enabled:
                    response_section = response_getter()

                    if response_section is not None:
                        self.__process_weather_section(
                            response_section,
                            base_results,
                            variables_key,
                            output_folder
                    )
        list(map(get_results,responses))

    def get_params(self) -> Dict[str, Any]:
        """
        Get the current API parameters configuration.
        
        Returns:
            Dictionary containing all configured API parameters
        """
        return self.params

import yaml
import os.path
import pandas as pd
from typing import NoReturn
from pandas.core.base import PandasObject
from df_custom_method import create_session_id

PandasObject.create_session_id = create_session_id


def load_settings(settings_path='./settings.yaml') -> dict:
    if not os.path.exists(settings_path):
        settings_path = settings_path
    with open(settings_path) as file_settings:
        settings = yaml.load(file_settings, Loader=yaml.FullLoader)
    return settings


def data_validation(service_users: pd.DataFrame,
                    required_columns: list) -> pd.DataFrame:
    for required_column in required_columns:
        service_users.dropna(subset=[required_column], inplace=True)
    return service_users


def data_preparation(service_users: pd.DataFrame) -> pd.DataFrame:
    service_users['timestamp'] = pd.to_datetime(service_users['timestamp'])
    return service_users


def main() -> NoReturn:
    settings = load_settings()
    service_users = pd.read_csv(settings['test_data_file'], delimiter=';')
    service_users = data_validation(service_users, settings['required_columns'])
    service_users = data_preparation(service_users)
    service_users = service_users.create_session_id(settings['time_delta_session'])
    service_users.to_csv(settings['test_result_file'])


if __name__ == '__main__':
    main()

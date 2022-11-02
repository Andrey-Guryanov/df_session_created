from pandas import DataFrame


def create_session_id(service_users: DataFrame,
                      time_delta_session: int) -> DataFrame:
    service_users = service_users.sort_values(['customer_id', 'timestamp'])
    service_users['timestamp_diff'] = service_users.groupby(
        "customer_id")["timestamp"].diff().dt.total_seconds()
    service_users['timestamp_diff'] = service_users['timestamp_diff'].fillna(0)
    service_users['new_session'] = False
    service_users.loc[
        ((service_users['timestamp_diff'] == 0)
         | (service_users['timestamp_diff'] > time_delta_session)),
        'new_session'] = True
    service_users['session_id'] = service_users['new_session'].cumsum()
    service_users.drop(['timestamp_diff', 'new_session'], axis=1)
    return service_users

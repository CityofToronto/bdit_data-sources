import json
from google.oauth2 import service_account

def _load_extra(conn_id: str, keyfile_path: str | None) -> dict:
    # credentials from file
    if keyfile_path:
        with open(keyfile_path) as f:
            data = json.load(f)
        return {"keyfile_dict": data}
    
    # credentials from airflow connection
    from airflow.sdk.bases.hook import BaseHook
    data = BaseHook.get_connection(conn_id).extra_dejson
    return {"keyfile_dict": data}

def get_google_credentials(conn_id: str = "google_sheets_generic", keyfile_path: str | None = None):
    extra = _load_extra(conn_id, keyfile_path)
    
    keyfile = extra["keyfile_dict"]
    info = json.loads(keyfile) if isinstance(keyfile, str) else keyfile
    
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    
    return service_account.Credentials.from_service_account_info(info, scopes=scopes)

# then connect with
# creds = get_google_credentials()
# or
# creds = get_google_credentials(keyfile_path=)

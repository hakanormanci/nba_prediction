from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from functools import lru_cache

# Configure retry strategy
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)

# Create session with retry strategy
nba_session = requests.Session()
adapter = HTTPAdapter(max_retries=retry_strategy)
nba_session.mount("http://", adapter)
nba_session.mount("https://", adapter)

# Configure session
nba_session.timeout = 60
nba_session.headers.update({
    'User-Agent': 'Mozilla/5.0',
    'Accept': 'application/json'
})

# Cache session getter
@lru_cache(maxsize=1)
def get_session():
    return nba_session
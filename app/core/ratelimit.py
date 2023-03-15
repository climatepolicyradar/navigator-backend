from slowapi.extension import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)

DEFAULT_LIMIT = "10000/second"

import os

# Database
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://superset:{os.environ.get('SUPERSET_DB_PASSWORD', 'superset')}"
    f"@superset_db:5432/superset"
)

# Cache (Redis)
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": "redis://superset_cache:6379/0",
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 3600,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_URL": "redis://superset_cache:6379/1",
}

# Security
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me-in-production-32chars!!")
WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False  # Set True in production with HTTPS

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ALERT_REPORTS": False,
}

# Allow Snowflake and ClickHouse database connections
PREVENT_UNSAFE_DB_CONNECTIONS = False

# Row limit for query results
ROW_LIMIT = 50000
VIZ_ROW_LIMIT = 10000

# Default dashboard refresh interval (seconds)
DEFAULT_DASHBOARD_REFRESH_FREQUENCY = 900  # 15 minutes

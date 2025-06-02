from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "HyperAds service APIs"


class DevSettings(Settings):
    HYP_USERS_SERVICE_BASE_URL: str = 'https://localhost:8000'

    # Other base URLs
    HYP_SERVICE_AUTHENTICATION_URL: str = HYP_USERS_SERVICE_BASE_URL + '/v1/service-auth'
    HYP_USER_AUTHENTICATION_URL: str = HYP_USERS_SERVICE_BASE_URL + '/v1/me'


class ProdSettings(Settings):
    HYP_USERS_SERVICE_BASE_URL: str = 'https://api.hyperinvento.com'

    # Other base URLs

    HYP_SERVICE_AUTHENTICATION_URL: str = HYP_USERS_SERVICE_BASE_URL + '/v1/service-auth'
    HYP_USER_AUTHENTICATION_URL: str = HYP_USERS_SERVICE_BASE_URL + '/v1/me'


prodSettings = ProdSettings()
devSettings = DevSettings()

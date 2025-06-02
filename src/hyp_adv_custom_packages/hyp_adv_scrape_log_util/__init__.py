# ruff: noqa

"""
This is a package used for scraping flows in the step function.
Ex: product detail scraping using ASIN

Has 2 components
-> logs requests
-> logs request events
"""

from . import requestEventsLogger as requestEventsLogger
from . import requestLogger as requestLogger

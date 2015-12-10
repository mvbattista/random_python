from celery.schedules import crontab
from celery.decorators import periodic_task

import airbnb_availability
import airbnb_listings

@periodic_task(
    run_every=(crontab(minute='30')),
    name="task_get_listing_availibility",
    ignore_result=True
)
def task_get_listing_availibility():
    """
    Gets availibility from Airbnb
    """
    aba = airbnb_availability.AirbnbAvailabilityParser()
    aba.crawl()

@periodic_task(
    run_every=(crontab(minute='0', hour='2')),
    name="task_get_listings",
    ignore_result=True
)
def task_get_listings():
    """
    Gets NYC Listings from Airbnb
    """
    abl = airbnb_availability.AirbnbListingsParser()
    abl.crawl()
import arrow
from clusterman_metrics import generate_key_with_dimensions

from clusterman.aws.client import ec2
from clusterman.aws.client import MAX_PAGE_SIZE


def spot_price_generator(start_time, end_time):
    """
    Generator for all spot prices changes that occurred between the start and end times.

    :param start_time: arrow object.
    :param end_time: arrow object.
    """
    # Convert to UTC since Amazon expects that.
    start_time = start_time.to('utc')
    end_time = end_time.to('utc')

    results = ec2.get_paginator('describe_spot_price_history').paginate(
        StartTime=start_time.datetime,
        EndTime=end_time.datetime,
        ProductDescriptions=['Linux/UNIX (Amazon VPC)'],
        PaginationConfig={'PageSize': MAX_PAGE_SIZE},  # limit the page size to help prevent SSL read timeouts
    )
    for result in results:
        for price in result['SpotPriceHistory']:
            # The history can include previous times, if the price is still in effect for the query period.
            # We've already recorded it, so ignore.
            if price['Timestamp'] < start_time:
                continue

            yield price


def write_prices_with_dedupe(spot_prices, writer, dedupe_interval_seconds):
    """
    Write spot price data from EC2 to the clusterman metrics client.

    :param spot_prices: iterable of spot price objects from EC2.
    :param writer: co-routine for writing to metrics client.
    :param dedupe_interval_seconds: int. Will write at most one price per interval per market.
    """
    # Prices are ordered by time, descending.
    # We take the price closest to the end of each interval.
    last_interval_written_by_market = {}
    for price in spot_prices:
        price_time = arrow.get(price['Timestamp'])

        # Round timestamp up to the nearest interval
        seconds_to_next_interval = (dedupe_interval_seconds - price_time.timestamp) % dedupe_interval_seconds
        next_interval = price_time.shift(seconds=seconds_to_next_interval)

        market = (price['InstanceType'], price['AvailabilityZone'])
        last_interval_written = last_interval_written_by_market.get(market, None)
        if not last_interval_written or next_interval < last_interval_written:
            last_interval_written_by_market[market] = next_interval
            data = get_data_for_price(price, next_interval)
            writer.send(data)


def get_data_for_price(price_object, recorded_minute):
    """
    Get price data in the format expected by the metrics client.

    :param price_object: dict of price data returned by EC2.
    :param recorded_minute: The minute the data should be recorded for, as an arrow object.
    """
    dimensions = {
        'aws_instance_type': price_object['InstanceType'],
        'aws_availability_zone': price_object['AvailabilityZone']
    }
    key = generate_key_with_dimensions('spot_prices', dimensions=dimensions)
    return (key, int(recorded_minute.timestamp), price_object['SpotPrice'])

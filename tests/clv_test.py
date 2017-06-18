import pytest
from src.clv import customer_info
from src.clv import time_frame
from datetime import datetime


def test_ci_get_amount():
	ci = customer_info('0')
	assert ci.get_amount("12.34 USD") == 12.34
	assert ci.get_amount("12.34 XXX") == 12.34
	assert ci.get_amount("12.3u4 XXX") == 0


def test_ci_ingest():
	ci = customer_info('1')
	event0 = {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e815502f", "event_time": "2017-01-06T12:45:52.041Z", "customer_id": "96f55c7d8f42", "tags": [{"some key": "some value"}]}
	ci.ingest(event0)
	assert ci.num_visits == 1
	event1 = {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "12.34 USD"}
	ci.ingest(event1)
	assert ci.orders['68d84e5d1a43'] == 12.34


def test_ci_ingest_visit():
	ci = customer_info('1')
	event0 = {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e815502f", "event_time": "2017-01-06T12:45:52.041Z", "customer_id": "96f55c7d8f42", "tags": [{"some key": "some value"}]}
	event1 = {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e815502g", "event_time": "2017-01-06T12:45:52.041Z", "customer_id": "96f55c7d8f42", "tags": [{"some key": "some value"}]}
	event2 = {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e815502h", "event_time": "2017-01-06T12:45:52.041Z", "customer_id": "96f55c7d8f42", "tags": [{"some key": "some value"}]}
	ci.ingest(event0)
	ci.ingest(event1)
	ci.ingest(event2)
	assert ci.num_visits == 3


def test_ci_ingest_order():
	ci = customer_info('1')
	event0 = {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "12.34 USD"}
	event1 = {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a44", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "12.34 USD"}
	event2 = {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a45", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "12.34 USD"}
	ci.ingest(event0)
	ci.ingest(event1)
	ci.ingest(event2)
	assert len(ci.orders) == 3
	assert sum(ci.orders.values()) == 3*12.34


def test_tf_parse_event_time():
	tf = time_frame()
	assert tf.parse_event_time('2017-01-06T12:46:46.384Z') == '2017-01-06'


def test_tf_get_start_time():
	tf = time_frame()
	tf.start_time = '2017-01-06'
	assert tf.get_start_time('2017-01-03') == '2017-01-03'
	assert tf.get_start_time('2017-07-03') == '2017-01-06'
	assert tf.get_start_time('2017-01-06') == '2017-01-06'
	assert tf.get_start_time('1999-11-20') == '1999-11-20'
	
	tf.start_time = None
	assert tf.get_start_time('2017-01-03') == '2017-01-03'


def test_tf_get_end_time():
	tf = time_frame()
	tf.end_time = '2017-12-10'
	assert tf.get_end_time('2017-01-03') == '2017-12-10'
	assert tf.get_end_time('2000-01-03') == '2017-12-10'
	assert tf.get_end_time('2050-10-01') == '2050-10-01'

	tf.end_time = None
	assert tf.get_end_time('2017-01-03') == '2017-01-03'


def test_tf_ingest():
	tf = time_frame()
	event = {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e815502f", "event_time": "2017-01-06T12:45:52.041Z", "customer_id": "96f55c7d8f42", "tags": [{"some key": "some value"}]}
	tf.ingest(event)
	assert tf.start_time == '2017-01-06'
	assert tf.end_time == '2017-01-06'



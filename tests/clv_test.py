import pytest
#from src import customer_info
#from src import time_frame
from src.clv import *
#from src.clv import top_x_simple_ltv_customers
#from src.clv import argument_parse
#from src.clv import event_iteration
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


def test_top_x():
	customer_map = dict()
	tf = time_frame()
	# customer 0
	event0 = {"type": "CUSTOMER", "verb": "NEW", "key": "0", "event_time": "2017-01-06T12:46:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AK"}
	event1 = {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e815502f", "event_time": "2017-01-06T12:45:52.041Z", "customer_id": "0", "tags": [{"some key": "some value"}]}
	event2 = {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "0", "total_amount": "100.5 USD"}
	# customer 1
	event3 = {"type": "CUSTOMER", "verb": "NEW", "key": "1", "event_time": "2017-01-06T12:46:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AK"}
	event4 = {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e815502f", "event_time": "2017-02-06T12:45:52.041Z", "customer_id": "1", "tags": [{"some key": "some value"}]}
	event5 = {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e815502f", "event_time": "2017-12-06T12:45:52.041Z", "customer_id": "1", "tags": [{"some key": "some value"}]}
	event6 = {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "1", "total_amount": "0.5 USD"}
	# customer 2
	event7 = {"type": "CUSTOMER", "verb": "NEW", "key": "2", "event_time": "2017-01-06T12:46:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AK"}
	event8 = {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e815502f", "event_time": "2017-6-06T12:45:52.041Z", "customer_id": "2", "tags": [{"some key": "some value"}]}
	event9 = {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "2", "total_amount": "20.5 USD"}
	# customer 3
	event10 = {"type": "CUSTOMER", "verb": "NEW", "key": "3", "event_time": "2017-01-06T12:46:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AK"}
	event11 = {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e815502f", "event_time": "2018-3-06T12:45:52.041Z", "customer_id": "3", "tags": [{"some key": "some value"}]}
	event12 = {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "3", "total_amount": "80.5 USD"}
	for event in [event0, event1, event2, event3, event4, event5, event6, event7, event8, event9, event10, event11, event12]:
		customer_map, tf = ingest(event, [customer_map, tf])
	top_3_simple_ltv_customers = top_x_simple_ltv_customers(3, [customer_map, tf])
	assert top_3_simple_ltv_customers == ['0', '3', '2']


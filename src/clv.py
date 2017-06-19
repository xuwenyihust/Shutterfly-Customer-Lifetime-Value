import os
import json
import argparse
from datetime import datetime
import heapq
#from customer_info import customer_info
#from time_frame import time_frame


class customer_info(object):

	def __init__(self, customer_id):
		# Customer ID
		self.customer_id = customer_id
		# Record all the orders
		# HashMap<order_id, amount>
		self.orders = dict()
		# Number of site visits
		self.num_visits = 0

	# The function that ingest event to the corresponding customer
	def ingest(self, event):
		# Append the site visit
		if event['type'] == 'SITE_VISIT':
			self.num_visits += 1
		# Append the expenditures
		elif event['type'] == 'ORDER':
			if event['verb'] == 'NEW':
				#self.orders[event['key']] = event['total_amount']
				self.orders[event['key']] = self.get_amount(event['total_amount'])
			elif event['verb'] == 'UPDATE':
				#self.orders[event['key']] = event['total_amount']
				self.orders[event['key']] = self.get_amount(event['total_amount'])
			# Inappropriate field content
			#else:

	# Extract money value
	def get_amount(self, total_amount):
		money_value = total_amount.split()[0]
		try:
			money_value = float(money_value)
			return money_value
		except:
			return 0

class time_frame(object):

	def __init__(self):
		self.start_time = None
		self.end_time = None

	def ingest(self, event):
		# Parse the time field
		event_time = event['event_time']
		event_date = self.parse_event_time(event_time)
		
		# Update the earlist time
		self.start_time = self.get_start_time(event_date)
		# Update the latest time
		self.end_time = self.get_end_time(event_date)

	# Parse the time field
	# %Y-%m-%d
	def parse_event_time(self, event_time):
		# Only need the year-month-date part
		return event_time.split('T')[0]

	# Select the earlist event time
	def get_start_time(self, event_date):
		# If self.start_time is None
		if not self.start_time:
			return event_date

		date0 = datetime.strptime(self.start_time, '%Y-%m-%d')
		date1 = datetime.strptime(event_date, '%Y-%m-%d')

		if date0 <= date1:
			return self.start_time
		else:
			return event_date

	# Select the latest event time
	def get_end_time(self, event_date):
		# If self.end_time is None
		if not self.end_time:
			return event_date

		date0 = datetime.strptime(self.end_time, '%Y-%m-%d')
		date1 = datetime.strptime(event_date, '%Y-%m-%d')
		
		if date0 <= date1:
			return event_date
		else:
			return self.end_time



# Ingest the coming event to customer_info & time_frame
def ingest(event, D):
	customer_map, tf = D

	# Check the event type
	if event['type'] == 'CUSTOMER':
		customer_id =  event['key']
	else:
		customer_id = event['customer_id']

	# New customer
	if customer_id not in customer_map:
		ci = customer_info(customer_id)
		customer_map[customer_id] = ci			
	# Existing customer
	else:
		ci = customer_map[customer_id]
	# Ingest the incoming event
	ci.ingest(event)
	tf.ingest(event)

	return customer_map, tf


def top_x_simple_ltv_customers(x, D):
	customer_map, tf = D
	# LTV = 52(a) X t
	# a: customer expenditures per visit (USD) x number of site visits per week
	time_delta = datetime.strptime(tf.end_time, '%Y-%m-%d') - datetime.strptime(tf.start_time, '%Y-%m-%d')
	week_delta = time_delta.days / 7
	customer_list = list(customer_map.values())
	# Use a heap to store all the ltv
	customer_ltv_heap = []

	# Iterate each customer:
	for customer in customer_list:
		order_list = list(customer.orders.values())
		customer_exp = sum(order_list)
		# Get # visists
		num_visit = customer.num_visits
		# Get customer expenditures per visit (USD)
		customer_exp_per_visit = customer_exp / num_visit
		# Get number of site visits per week
		num_visit_per_week = num_visit / week_delta
		# Get LTV
		a = customer_exp_per_visit * num_visit_per_week
		t = 10
		ltv = 52 * a * t
		# Add the LTV to heap	
		heapq.heappush(customer_ltv_heap, (-1*ltv, customer.customer_id))

	res = []
	for i in range(min(x, len(customer_ltv_heap))):
		x = heapq.heappop(customer_ltv_heap)[1]
		res.append(x)
	
	return res	
	

def argument_parse():
	# Parse the given arguments
	parser = argparse.ArgumentParser()
	parser.add_argument("-i", help="input data")
	parser.add_argument("-o", help="output data")
	args = parser.parse_args()
	return args


def event_iteration(events_file, customer_map, tf):
	with open(os.getcwd() + '/' + events_file) as input_file:
		# Load the k-v pairs in input file as json objects
		events = json.load(input_file)
		for event in events:
			customer_map, tf = ingest(event, [customer_map, tf])

	return customer_map, tf


def main():
	# Get the parsed arguments	
	args = argument_parse()

	# Create a hashmap to direct customer_id to customer_info object
	customer_map = dict()	
	# Initialize the time_frame
	tf = time_frame()

	# Parse the input events
	events_file = args.i
	# Iterate each incoming event
	customer_map, tf = event_iteration(events_file, customer_map, tf)

	print(len(customer_map))
	print(tf.start_time)
	print(tf.end_time)

	top_3_simple_ltv_customers = top_x_simple_ltv_customers(3, [customer_map, tf])
	print(top_3_simple_ltv_customers)


if __name__ == "__main__":
	main()


import os
import json
import argparse


class customer_info(object):

	def __init__(self, customer_id):
		# Customer ID
		self.customer_id = customer_id
		# Sum of customer expenditures
		#self.sum_expenditures = 0
		# Record all the orders
		# HashMap<order_id, amount>
		self.orders = dict()
		# Number of site visits
		self.num_visits = 0
		# Record all the visits
		#self.visits = []
		# Time of fisrt event of this customer
		self.start_time = None
		# Time of last event of this customer
		self.end_time = None

	# The function that ingest event to the corresponding customer
	def ingest(self, event):
		# Append the site visit
		if event['type'] == 'SITE_VISIT':
			self.num_visits += 1
		# Append the expenditures
		elif event['type'] == 'ORDER':
			if event['verb'] == 'NEW':
				self.orders[event['key']] = event['total_amount']
			elif event['verb'] == 'UPDATE':
				self.orders[event['key']] = event['total_amount']
			# Inappropriate field content
			#else:


def main():
	# Parse the given arguments
	parser = argparse.ArgumentParser()
	parser.add_argument("-i", help="input data")
	parser.add_argument("-o", help="output data")
	args = parser.parse_args()

	# Create a hashmap to direct customer_id to customer_info object
	customer_map = dict()	

	# Parse the input events
	events_file = args.i
	with open(os.getcwd() + '/' + events_file) as input_file:
		# Load the k-v pairs in input file as json objects
		events = json.load(input_file)	
		for event in events:
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

	#for key in customer_map:
	#	print(key)

if __name__ == "__main__":
	main()


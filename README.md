[![Python](https://img.shields.io/badge/python-3.4-blue.svg)]()
# Shutterfly-Customer-Lifetime-Value
Coding challenge from shutterfly.

## Overview
Calculate simple LTV using equation: `52(a) x t`. `t` is 10 years here. `a` = customer expenditures per visit (USD) x number of site visits per week. A customer's expenditures is the sum of all expenditures of this customer, and the time of visit can be extracted from all his/her visit events. The timeframe is calculated by substracting the earliest time from the latest time, which are recorded from all event times.

## Running
Run the script from the project home directory:
`. src/clv.sh`

## Testing
Use pytest for testing
`pytest tests/clv_test.py`

## Design Desisions & Performance
* `customer_info` & `time_frame`

  The main 2 classes are `customer_info` & `time_frame`. `customer_info` stores all the required data of each customer, including customer id, a hashmap<order_id, amount> and the total number of visists. `time_frame` stores the very earliest time and the very latest time. 

* LTV sort

  A max heap (by X(-1) in a min heap) will be used for sorting all the LTVs.
  
* Event iteration

  Currently, all the events are processed one by one. Concurrecny techs may be utilized to improve performance in the future.
  
* `Ingest()`

  The `Ingest()` method take constant time.
  
* `TopXSimpleLTVCustomers`

  The `TopXSimpleLTVCustomers` method takes O(n * logn) time(n: # customers).
  

## Edge Cases
* Invalid order amount

  The expected order amount format is `float + ' ' + USD`. If invalid order amount is identified, it will be treated as 0.

* Update a non-existing order

  When an 'UPDATE' order event occurs, it assumes the prerequisit the there has been an existing order with the same order id. If it doesn't meet this prerequisit, we assume the 'UPDATE' was a typo and treat it as a 'NEW' order. 

* Update a non-existing customer

  Similar to the order one, we treat an 'UPDATE' to a non-existing customer as a 'NEW' operation.

## Requirements
* pytest

## License
See the LICENSE file for license rights and limitations (MIT).

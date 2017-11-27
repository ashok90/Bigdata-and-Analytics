#######infra setup
#
#conda install numpy
#pip install pystan 				#http://pystan.readthedocs.io/en/latest/installation_beginner.html
#conda install -c conda-forge fbprophet. #https://facebook.github.io/prophet/docs/installation.html



#pip install coinbase
from coinbase.wallet.client import Client


import json
from coinbase.wallet.client import Client
client = Client('','');
rates = client.get_exchange_rates(currency='BTC')

import json
f = csv.writer(open("test.csv", "wb+"))

x=json.loads(rates)
for x in x:
	f.writerow([x["currency"],x["rates"]])


with open('data.txt', 'a') as outfile:
     json.dump(rates1,outfile)

# lst-openfigi
This set of scripts use the OpenFIGI API to search for and download instrument the OpenFIGI instrument identifier using other standard identifiers like exchange ticker, ISIN etc.
The OpenFIGI identifier is designed to be immutable so makes it much easier to track instruments which have had ticker changes.

It is possible to use this API anonymously but it is recommended to register for a free API key as it will allow for much higher rate limit for API requests. This can be done at [OpenFIGI sign-up](https://www.openfigi.com/user/signup).

## ticker_change_check.py
This script references the file equities-attributes.csv. This file contains properties for equities which link a open_figi to ticker, cik, cusip, isin etc. It compares the ticker associated with the open_figi currently using the OpenFIGI API to the ticker in this file - if there are any differences it is highlighted to allow the changes to be updated.

## figi_lookup.py
This file takes in a list of tickers and uses the API to download the open_figi associated with them. There are tickers from multiple regions so different loops are used for each one to allow the exchange code for the ticker to be included in the query to limit the potential results.
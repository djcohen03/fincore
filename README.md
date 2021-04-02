# fincore
This is a repository built around the alphavantage [API](https://www.alphavantage.co) for building, storing, and managing a financial-data database.

## Initialization:
- Create a database for storage.  I would recommend running a postgres server in a docker container (see [here](https://hackernoon.com/dont-install-postgres-docker-pull-postgres-bee20e200198))
- Set the string value of `dbpath` in the `db/session` file to point towards your target database
- Get a free API Key [here](https://www.alphavantage.co/support/#api-key)
- Add a file `db/api_key.py`, and simply add one line: `API_KEY = '[Enter API Key Here]'`. Make sure this is kept private!
- Run `./bin/dbinit` from the command line. This should initialize your database and add some seed data
- Run `./bin/fetch` from the command line to run your first fetch.  If all works as planned, this should take a bit of time to fetch all the data from alphavantage, since there is a 4-request-per-minute limit for free users

These steps should get your local financial database up and running, and should give it some data to work with right away.


## Example Usage
```
Todo...

```

# stocks-scraper
Service that collects stock's prices from T-Invest API and enrich DB with it.

## Preconditions

1. Create `./configs/config.yaml` with stocks-scraper config
```yaml
instruments_id:                  # list of instruments to consume candles
  - example1
  - example2
use_moex_index_instruments: true # if specified use MOEX Index instruments as instrument_id
from: 2013-01-01T00:00:00Z       # date to start scraping from (the date of the first minute candle is also taken into account)
port: 8082                       # port on which server will be started
```

2. Create `./configs/invest.yaml` with invest-api-go-sdk config, as specified here: https://github.com/RussianInvestments/invest-api-go-sdk.
Token is provided through `T_INVEST_API_TOKEN` env variable.

# Functionality
 
- Scraper start scrap stocks for provided `instruments_id` list. 
  - It saves price to `stocks` table in DB.
  - Also saves instrument ids and first candles occurrences dates to `instruments` table.
- Scraper also has own API.
    - `GET /check?instrument_id=example` - checks whether `instrument_id` stocks are fullfilled or not.
      (returns json `{"ok":true, "exist":true}`, where `ok` means ready to use and `exist` tells about instrument existence in scraper)
    - `POST /scrap?instrument_id=example&cmd=add` - add or delete instrument_id to/from scraping. (cmd can be `delete` or `add`)

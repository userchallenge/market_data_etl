# Question no the config-part: 
I don't follow the logic for the Config-files. Isn't it better to have a config-file with all config in so I don't have to deploy code when changing config? Please explain so I understand. 

# Config-clarification:
CurrencyConfig: I expect the system to fetch any currency data from the Yfinance-API, hence it should just accept any currency coming from that API
EconomicIndicatorConfig: Please extract the data from economic_data_mapping.csv and have that in the config similar to what you do in APIEnndpointConfig.

# Architecture Enhancements-clarification
CAn you explain the pros and cons for this before I decide?

# Answer to your questions:
1: Semi-Static values should always be in config files. My view is that I will use date ranges when populating data and that's the only use of date ranges. I dont understand why date-ranges should be in config, please explain. 
2: Yes, but please explain how you think so I don't oversee something important
3: Can be removed
4: See my comment on Config-clarification
5: Please oversee the database schema as well
6: yes, that's important. I want these tests to be in pytest

------------------

Comments regarding Backward Compatibility Strategy

General: It seems you're playing it a bit safe. Since I've asked you to write pytests, I assume you will be able to ensure new database schema works, that all CLI-arguments still work, all config is loaded correctly, etc. 

1. CLI Commands: I want all existing CLI commands to work as they are currently do. And just for your info: You are always bringing up old and erroneous flags such as source and indicator although they were removed a long time ago. How can we remove them from your memory?
1. CLI Commands: Keep exact same command signatures 
  # These must work exactly the same
  market-data-etl fetch-prices --ticker AAPL --from 2024-01-01
  market-data-etl fetch-economic --source eurostat --indicator prc_hicp_midx

Don't change these documents: database_improvements_spec.md, improv.md, notes.md



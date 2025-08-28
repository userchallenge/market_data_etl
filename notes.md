CLI fetching all, indicator, country, market, 
indikator, land/område, ta bort tidsaspekten helt från databasen

I need to refactor how economic indicators are named to make it more generic. Any indicator shall have a generic name that 

make a description of all CLI-commands, but no other documentation needed
no need for alias backup of the old names
change CLI to ask for --indicator (the new indicator name without area, e.g. unemployment_rate) and --area (can be country or area)
create pytests for all CLI-commands, I want to have a way of doing end-to-end tests based on the CLI-commands
you can remove the "rate" from the indicators, let's leave interest, unemployment and inflation. I will know what it means. 
You don't need to do time estimations

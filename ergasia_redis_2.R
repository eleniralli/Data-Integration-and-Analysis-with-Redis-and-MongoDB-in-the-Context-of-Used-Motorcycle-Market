# If the redux package is not installed, we uncomment the following line to install it.
#install.packages("redux")

# Load the redux package into the R session.
# The redux package provides a set of functions to interact with Redis databases using the hiredis library.
library("redux")

# Establishing a local connection to a Redis server
# The `hiredis` function from the redux package is used to create a connection object.
# This connection object will be used for subsequent Redis operations (commands).
# The `redis_config` function is used to specify the configuration settings for the connection.
# Here, 'host' is set to "127.0.0.1" which is the IP address for localhost, indicating that
# the Redis server is running on the same machine as the R session.
# The 'port' is set to "6379", which is the default port number that Redis servers listen on.
r <- redux::hiredis(
  redux::redis_config(
    host = "127.0.0.1",
    port = "6379"))

# The result, 'r', is a Redis connection object that you can use to interact with the Redis server.
# For example, to set a key-value pair, retrieve data, or perform other Redis commands.


# Installation of necessary packages (if they have not already been installed)
#if (!require(bit64)) install.packages("bit64")
#if (!require(dplyr)) install.packages("dplyr")

# Loading packages
library(bit64)
library(dplyr)


# Loading data from CSV
# Loads the 'emails_sent.csv' dataset into R
emails_sent <- read.csv("D:\\redis\\ergasia_redis\\RECORDED_ACTIONS\\emails_sent.csv")
# Loads the 'modified_listings.csv' dataset into R
modified_list <- read.csv("D:\\redis\\ergasia_redis\\RECORDED_ACTIONS\\modified_listings.csv")




#1.1

# Subsetting 'modified_list' to include only entries from January
# Selecting only 'UserID' and 'ModifiedListing' columns
January <- subset(modified_list, MonthID == 1, select=c(UserID, ModifiedListing))

# Subsetting 'modified_list' to include only entries from February
# Selecting only 'UserID' and 'ModifiedListing' columns
February <- subset(modified_list, MonthID == 2, select=c(UserID, ModifiedListing))

# Subsetting 'modified_list' to include only entries from March
# Selecting only 'UserID' and 'ModifiedListing' columns
March <- subset(modified_list, MonthID == 3, select=c(UserID, ModifiedListing))

# Loop through each row of the 'January' data frame
for (i in 1:nrow(January)){
  # For each user (based on 'UserID'), set a bit at the position of 'ModifiedListing' value
  # in the bitmap named "ModificationsJanuary" in Redis.
  # This creates or modifies a bitmap for each user where each bit represents whether
  # a modification occurred (1) or not (0).
  r$SETBIT("ModificationsJanuary", January[i,1], January[i,2])
}

# Count and return the number of set bits (value of 1) in the bitmap "ModificationsJanuary".
# This provides the total number of modifications that happened in January.
r$BITCOUNT("ModificationsJanuary")



#1.2

# Perform a bitwise NOT operation on the bitmap "ModificationsJanuary" and store the result in "JanuaryNotModified".
# The NOT operation will invert the bits in the bitmap: bits that are 0 become 1, and bits that are 1 become 0.
r$BITOP("NOT", "JanuaryNotModified", "ModificationsJanuary")

# Count and return the number of set bits (value of 1) in the bitmap "JanuaryNotModified".
# After the NOT operation, this count will represent the number of "non-modifications" or the
#inverse of the original modifications bitmap for January.
r$BITCOUNT("JanuaryNotModified")



#1.3
# Load the dplyr package for data manipulation
library("dplyr")

# Select distinct rows from the 'emails_sent' data frame based on the UserID and MonthID
# Keep all columns from the second to the fourth
emails <- emails_sent[, 2:4] %>% distinct(UserID, MonthID, .keep_all = TRUE)

# Subset the 'emails' data frame for each of the first three months of the year
EmailsJanuary <- subset(emails, MonthID == 1)
EmailsFebruary <- subset(emails, MonthID == 2)
EmailsMarch <- subset(emails, MonthID == 3)

# Insert data into Redis. For each month, set a bit for each UserID that received an email
# The loop goes through each row of the January emails and sets a bit in Redis
for (i in 1:nrow(EmailsJanuary)) {
  r$SETBIT("EmailsSentJanuary", EmailsJanuary$UserID[i], "1")
}
# Repeat the process for February
for (i in 1:nrow(EmailsFebruary)) {
  r$SETBIT("EmailsSentFebruary", EmailsFebruary$UserID[i], "1")
}
# And repeat the process for March
for (i in 1:nrow(EmailsMarch)) {
  r$SETBIT("EmailsSentMarch", EmailsMarch$UserID[i], "1")
}

# Perform a bitwise AND operation on the three Redis keys (bitmaps) representing
# emails sent in January, February, and March, respectively
# Store the result in a new Redis key named "Task 1.3"
r$BITOP("AND", "Task 1.3", c("EmailsSentJanuary", "EmailsSentFebruary", "EmailsSentMarch"))

# Count and return the number of set bits in the "Task 1.3" bitmap
# This represents the number of users who received an email in all three months
r$BITCOUNT("Task 1.3")

# Print out the count of users who received at least one email each month
print(paste("The number of users who received at least one email every month is:", r$BITCOUNT("Task 1.3")))







#1.4

# Perform a bitwise NOT operation on the bitmap for emails sent in February, creating an inverted bitmap.
# In the inverted bitmap, a set bit (1) will now represent a user who did NOT receive an email in February.
r$BITOP("NOT", "InvertedEmailsSentFebruary", "EmailsSentFebruary")

# Perform a bitwise AND operation between the bitmaps for emails sent in January, the inverted bitmap for February,
# and the bitmap for emails sent in March. This will result in a bitmap where a set bit represents users who
# received emails in January and March but NOT in February.
r$BITOP("AND", "Task 1.4", c("EmailsSentJanuary", "InvertedEmailsSentFebruary", "EmailsSentMarch"))

# Count and return the number of set bits in the "Task 1.4" bitmap.
# This count represents the number of users who received an email in January and March but NOT in February.
r$BITCOUNT("Task 1.4")

# Print out the result with a descriptive message.
# This prints the number of users who met the criteria of receiving an email in January and March but not in February.
print(paste("The number of users who received an email in January and March but NOT in February is:", r$BITCOUNT("Task 1.4")))







#1.5


# Subset 'emails_sent' for January only and select the 'UserID' and 'EmailOpened' columns
EmailsJanuary_agg <- subset(emails_sent, MonthID == 1, select = c(UserID, EmailOpened))

# Aggregate the data by 'UserID' and sum up the 'EmailOpened' column
# This gives us the total number of emails opened by each user in January
EmailsJanuary_agg <- aggregate(EmailsJanuary$EmailOpened, by = list(UserID = EmailsJanuary$UserID), FUN = sum)

# Convert the 'x' column to a binary indicator: 1 if any email was opened, 0 if no emails were opened
EmailsJanuary_agg$x <- if_else(EmailsJanuary_agg$x == 0, 0, 1)

# Loop through the aggregated January emails and set a bit for each user ID based on whether they opened any emails
# We're using the result of the aggregation to set the bits in the bitmap "EmailsOpenedJanuary" in Redis
for (i in 1:nrow(EmailsJanuary_agg)) {
  r$SETBIT("EmailsOpenedJanuary", EmailsJanuary_agg$UserID[i], EmailsJanuary_agg$x[i])
}

# Perform a bitwise NOT operation on "EmailsOpenedJanuary" to get "EmailsNotOpenedJanuary"
# In the resulting bitmap, a set bit will now represent a user who did NOT open an email in January
r$BITOP("NOT", "EmailsNotOpenedJanuary", "EmailsOpenedJanuary")

# Perform a bitwise AND operation between "EmailsNotOpenedJanuary" and "ModificationsJanuary"
# The resulting bitmap, "Task 1.5", will represent users who did not open any emails and who updated their listing in January
r$BITOP("AND", "Task 1.5", c("EmailsNotOpenedJanuary", "ModificationsJanuary"))

# Count and return the number of set bits in the "Task 1.5" bitmap
# This count represents the number of users who received but did not open an email and also made a listing update
r$BITCOUNT("Task 1.5")

# Print out the count with a descriptive message
print(paste("The number of users who received but did not open an email in January and updated their listing is:", r$BITCOUNT("Task 1.5")))








#1.6

# For February, subset the 'emails_sent' data frame to get user IDs and whether they opened emails.
EmailsFebruary_agg <- subset(emails_sent, MonthID == 2, select = c(UserID, EmailOpened))
# Aggregate by UserID to sum up EmailOpened values, which indicates whether a user opened any email.
EmailsFebruary_agg <- aggregate(EmailsFebruary$EmailOpened, by = list(UserID = EmailsFebruary$UserID), FUN = sum)
# Convert the sum into a binary indicator where 0 remains 0 and any positive number becomes 1.
EmailsFebruary_agg$x <- if_else(EmailsFebruary_agg$x == 0, 0, 1)

# For March, repeat the process of subsetting and aggregating email open data.
EmailsMarch_agg <- subset(emails_sent, MonthID == 3, select = c(UserID, EmailOpened))
EmailsMarch_agg <- aggregate(EmailsMarch$EmailOpened, by = list(UserID = EmailsMarch$UserID), FUN = sum)
EmailsMarch_agg$x <- if_else(EmailsMarch_agg$x == 0, 0, 1)

# For each user in February, set a bit in Redis based on whether they modified their listing.
for (i in 1:nrow(February)) {
  r$SETBIT("ModificationsFebruary", February$UserID[i], February$ModifiedListing[i])
}

# For each user in February, set a bit in Redis based on whether they opened an email.
for (i in 1:nrow(EmailsFebruary_agg)) {
  r$SETBIT("EmailsOpenedFebruary", EmailsFebruary_agg$UserID[i], EmailsFebruary_agg$x[i])
}

# Perform a NOT operation on February's opened emails to track emails not opened.
r$BITOP("NOT", "EmailsNotOpenedFebruary", "EmailsOpenedFebruary")
# Perform an AND operation to find users who did not open emails and modified listings in February.
r$BITOP("AND", "February", c("EmailsNotOpenedFebruary", "ModificationsFebruary"))
# Count the number of users who fit the criteria in February.
r$BITCOUNT("February")

# Repeat the process for March for modifications and email openings.
for (i in 1:nrow(March)) {
  r$SETBIT("ModificationsMarch", March$UserID[i], March$ModifiedListing[i])
}
for (i in 1:nrow(EmailsMarch_agg)) {
  r$SETBIT("EmailsOpenedMarch", EmailsMarch_agg$UserID[i], EmailsMarch_agg$x[i])
}
r$BITOP("NOT", "EmailsNotOpenedMarch", "EmailsOpenedMarch")
r$BITOP("AND", "March", c("EmailsNotOpenedMarch", "ModificationsMarch"))
r$BITCOUNT("March")

# Perform an OR operation between the results of January (from previous script), February, and March.
r$BITOP("OR", "Task 1.6", c("Task 1.5", "February", "March"))
# Count the number of users who fit the OR criteria across all three months.
r$BITCOUNT("Task 1.6")

# Print the final count of users who received but did not open emails and updated their listings across the three months.
print(paste("The number of users who received but did not open an email in January and updated their listing is:", r$BITCOUNT("Task 1.6")))



#1.7

# Perform a bitwise AND operation between the bitmap for users who opened emails in January and
# the bitmap for users who modified their listings in January.
# The resulting bitmap "OpenedModJan" will have bits set only for users who did both.
r$BITOP("AND", "OpenedModJan", c("EmailsOpenedJanuary", "ModificationsJanuary"))

# Perform a bitwise AND operation between the bitmap for users who opened emails in February and
# the bitmap for users who modified their listings in February.
# The resulting bitmap "OpenedModFeb" will have bits set only for users who did both.
r$BITOP("AND", "OpenedModFeb", c("EmailsOpenedFebruary", "ModificationsFebruary"))

# Perform a bitwise AND operation between the bitmap for users who opened emails in March and
# the bitmap for users who modified their listings in March.
# The resulting bitmap "OpenedModMarch" will have bits set only for users who did both.
r$BITOP("AND", "OpenedModMarch", c("EmailsOpenedMarch", "ModificationsMarch"))

# Count the number of bits set in the "OpenedModJan" bitmap. This number represents users who both
# opened an email and modified their listing in January.
r$BITCOUNT("OpenedModJan")

# Count the number of bits set in the "OpenedModFeb" bitmap. This number represents users who both
# opened an email and modified their listing in February.
r$BITCOUNT("OpenedModFeb")

# Count the number of bits set in the "OpenedModMarch" bitmap. This number represents users who both
# opened an email and modified their listing in March.
r$BITCOUNT("OpenedModMarch")




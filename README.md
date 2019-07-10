# BNP-Project
Test for the BNP Data Engineer Position

Notes:
1) I tried to modularize as much as I can
2) For the Time Series visualizations (part 2,3), a monthly resampling has been performed for top 20 senders, although Seaborn chooses to display the data by a 6-month pattern. This is probably in order not to overpopulate the x-axis.
3) All three visualizations are exported to separate folders, as verified in the command line during the script execution. 
4) For the final part (part 3), both the absolute value (unique senders per person) as well as the relative value (emails sent/unique senders) plots are exported

5) In the final chart (ratio chart), 'blank' has received 0 messages, and is therefore omitted from  the chart (thus size 19). For the rest, because either/both of the two variables that participate in the ratio could be zero (a/0 and 0/0 cases), I added +1 to both variables to avoid such cases.

6) The script runtime is measured and displayed when it is finished.

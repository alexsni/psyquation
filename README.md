# psyquation

I haven't made any external executables, e.g. running on real distributed spark environment, I can set it up if needed also running on spark in k8s.
That would require few steps:
* adding addition input parameters for sourcing `ds1.csv` and `ds2.csv`
* modifying `setupReaders` method in `App15MinIntervals` to accept these parameters.
* updating spark dependency in `pom.xml` to be provided for test scope only.

So for running the examples just check out the code to your IDE, and run main classes:
* `App15MinIntervals` - first task, input parameters `"2018-03-23T00:00:00" "2018-03-26T00:00:00" "path/15_min_out.json"`
* `App1HourInterval` - second task, input parameters `"/Users/osnisar/alexsni/psyquation/first/15_min_out.json" "/Users/osnisar/alexsni/psyquation/final_out.json" `(first parameter should match the same in the first app).

`App15MinIntervals` - reads data from the local resources folder for simplicity, though it builds up app constrain. If needed I can add addition parameters for this, but for demonstration purpose I think it is ok.

There are also few tests added to cover just main things.

Also spark would broadcust small dataframes out of the box, so I didn't covered this implicitly. For large dataframes I would be possible to play around with broadcust size configuration, of going with salting approach. But for this demonstration purpose I didn't implemented this, as the task seems to be not aiming reformance tunning (based on small datasets provided).

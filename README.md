# NepalCompaniesScraper

A .NET 8 program that enumerates registration numbers from 1 to 1 million to collect information on all the companies registered in Nepal, and consolidates the data into a SQLite DB ([link](https://application.ocr.gov.np/faces/CompanyDetails.jsp))

I tested a few methods to maximize the number of simultaneous requests and the speed of completion of the operation:
- ### Sequential for loop
   Slowest of the bunch as expected.
  
- ### Naive Parallel.For()
   Fires off too many requests at the same time, chokes the CPU as well as the server and gets immediately throttled. Unless restricted using a severely low `MaxDegreeOfParallelism`, many failed probes.
  
- ### Task.WhenAll()
  Near-optimal simultaneous requests. Not all tasks (all 1M probe requests) executed simultaneously, certainly not as many as Parallel.For(). Only a sustainable (as defined by the runtime) number of requests fired at one time, restrained by available CPU cores/threads as well as by the delay in asynchronous response from the server. Concurrency controllable by use of `Semaphore`
  
- ### Parallel.ForSync()
  Near-optimal simultaneous requests. Pushes the yield from Task.WhenAll() at the cost of a few extra failed probes. CPU allocation easily configurable via `MaxDegreeOfParallelism`, though a lot more robust than Parallel.For() (probably because of first-class support for asynchronous programming model with async-await). Performance maxed out at `MaxDegreeOfParallelism = 200` with a reasonable failure count; any more and the failure rate crept up.

> ## Note:
> Except for the Parallel.For() method, which has the earliest **unencapsulated** implementation of the registration number probe logic that doesn't include any contingency for failures including any form of sleep-then-retry flow, the rest have the asynchronous `HarvestCompanies` method to call on

> Each probe to a registration number requires two HTTP calls - one GET to obtain the required jsession cookie and the form parameters along with a new instance of `HttpClient`, the other a POST to make the actual call to check the registration number. Though the same `HttpClient` object could probably be shared for a number of POSTs, this was decided against - in light of the fact that during the Parallel.For() implementation, companies obtained via a request for one registration number had a completely different value for their registration number field. I surmised that there must be some mixup going on due to the same `HttpClient` object being used for multiple simultaneous requests of different registration numbers, hence the separate objects for each probe. Resolving this issue would undoubtedly allow faster results since not every probe would require two HTTP requests

## Some rough benchmarks (2 minute window for all):
<table>
  <th>Method</th>
  <th>Processed reg. numbers</th>
  <th>Harvested companies</th>
  <th>Failed reg. numbers</th>
  <th>Remarks</th>
  <tr>
    <td>Parallel.ForAsync()</td>
    <td>3483</td>
    <td>5777</td>
    <td>2</td>
    <td>MaxDegreeOfParallelism = 100</td>
  </tr>
  <tr>
    <td>Parallel.ForAsync()</td>
    <td>2972</td>
    <td>2829</td>
    <td>635</td>
    <td>MaxDegreeOfParallelism = 1000</td>
  </tr>
  <tr>
    <td>Parallel.ForAsync()</td>
    <td>2434</td>
    <td>4529</td>
    <td>13</td>
    <td>MaxDegreeOfParallelism = 200</td>
  </tr>
  <tr>
    <td>Parallel.ForAsync()</td>
    <td>2956</td>
    <td>5083</td>
    <td>9</td>
    <td>MaxDegreeOfParallelism = 200 (Repeat sample)
    </td>
  </tr>
  <tr>
    <td>Parallel.ForAsync()</td>
    <td>2737</td>
    <td>5005</td>
    <td>1</td>
    <td>MaxDegreeOfParallelism = 80</td>
  </tr>
  <tr>
    <td>Parallel.ForAsync()</td>
    <td>2315</td>
    <td>4627</td>
    <td>0</td>
    <td>MaxDegreeOfParallelism not specified (auto)</td>
  </tr>
  <tr>
    <td>Parallel.ForAsync()</td>
    <td>3194</td>
    <td>5416</td>
    <td>12</td>
    <td>MaxDegreeOfParallelism = 120</td>
  </tr>
  <tr>
    <td>Parallel.ForAsync()</td>
    <td>2875</td>
    <td>5114</td>
    <td>7</td>
    <td>MaxDegreeOfParallelism = 110</td>
  </tr>
  <tr>
    <td>Parallel.ForAsync()</td>
    <td>12080</td>
    <td>14792</td>
    <td>0</td>
    <td>MaxDegreeOfParallelism = 110. Faster network</td>
  </tr>
  <tr>
    <td>Parallel.ForAsync()</td>
    <td>12170</td>
    <td>14799</td>
    <td>1</td>
    <td>MaxDegreeOfParallelism = 200. Faster network</td>
  </tr>
  <tr>
    <td>Parallel.ForAsync()</td>
    <td>8595</td>
    <td>10749</td>
    <td>0</td>
    <td>MaxDegreeOfParallelism = 500. Faster network</td>
  </tr>
  <tr>
    <td>Parallel.ForAsync()</td>
    <td>8911</td>
    <td>11594</td>
    <td>0</td>
    <td>MaxDegreeOfParallelism not specified (auto). Faster network</td>
  </tr>
  <tr>
    <td>Parallel.ForAsync()</td>
    <td>7159</td>
    <td>8715</td>
    <td>1</td>
    <td>MaxDegreeOfParallelism = 1000. Faster network. Saw the tasks waiting long (>10 s) for the server's response for three times, each wait longer than the previous.</td>
  </tr>
  <tr>
    <td>Task.WhenAll()</td>
    <td>9008</td>
    <td>11577</td>
    <td>0</td>
    <td>SemaphoreSlim(100_000). I know, no sense in saying 100k tasks at the same time. Faster network</td>
  </tr>
  <tr>
    <td>Task.WhenAll()</td>
    <td>9143</td>
    <td>11848</td>
    <td>0</td>
    <td>SemaphoreSlim(100_000). Repeat sample. Faster network</td>
  </tr>
  <tr>
    <td>Task.WhenAll()</td>
    <td>9757</td>
    <td>12504</td>
    <td>1</td>
    <td>No SemaphoreSlim. Technically, all tasks at once; practically, balanced by the runtime. Faster network</td>
  </tr>
</table>

## Remarks on the benchmarks:
- Even for the same parameters, not the same registration numbers might have been chosen in the 2-minute window that the tests were allowed to run. As a result, the failed cases might not have been entirely due to too many requests being fired at once or some server-side throttling but due to pure chance of happening upon dud registration numbers i.e. those numbers that do **not** have (a) corresponding company(ies) (yes, that is a valid case)
- The **Processed** column gives a better sense of performance than the **Harvested** column for the same reason mentioned above.
- By far the biggest discriminant between observations is the network speed.
- For Parallel.ForAsync(), higher `MaxDegreeOfParallelism` doesn't mean better throughput (Processed) _or_ yield (Harvested). At some point, the CPU chokes (though not to the same extent as naive Parallel.For(), which is clearly not designed for asynchronous tasks) and/or the server throttles the requests, leading to greater failure rates and increased response times.
- For Task.WhenAll(), you basically can't break it (I put in 100_000 for max. simultaneous locks, but that basically meant no restraint). It appears to be self-balancing even when you don't impose a limit on concurrency manually using `Semaphore` or similar locking mechanisms.
- The `200`, `500` and `auto` rows for Parallel.ForAsync()'s `MaxDegreeOfParallelism` are interesting. The `auto` and the `500` rows have similar numbers but the `200` row has substantially better metrics. This could be down to the fact that the former two runs were carried out after the latter, which could have led to the server wisening up to the traffic from my IP and slowing down responses. `auto` is supposed to mean the number of cores available, so that's not a very large number compared to the other test cases; so given the relative similarity with higher parallelism observations, it is likely a server-side effect.

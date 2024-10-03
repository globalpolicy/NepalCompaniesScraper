#define TASK_WHENALL_WITH_DBSTORE

using System.Collections.Concurrent;
using System.Net.Http.Headers;
using HtmlAgilityPack;
using System.Net;
using System.Web;
using System.Diagnostics;
using System.Net.Http;
using System.Runtime.CompilerServices;

namespace NepalCompaniesScraper
{
	internal class Program
	{
		private static string targetUrl = "https://application.ocr.gov.np/faces/CompanyDetails.jsp";

		static void Main(string[] args)
		{


			ConcurrentBag<int> failedRegNumbers = new ConcurrentBag<int>(); // thread-safe collection of company reg. numbers that failed to get a response
			ConcurrentDictionary<int, List<string[]>> harvestedCompanies = new ConcurrentDictionary<int, List<string[]>>(); // thread-safe company-reg-no.:List of [{SN},{NepaliName},{EnglishName},{RegNo},{RegdDate},{CompanyType},{Address},{LastContacted}]
			ConcurrentBag<int> processedRegNumbers = new ConcurrentBag<int>(); // thread-safe collection of company reg. numbers that have been processed

			// enumerate over all possible company registration numbers
			Stopwatch stopwatch = new Stopwatch();
			stopwatch.Start();

#if PARALLELFOR

			Parallel.For(1, 1_000_000, new ParallelOptions { MaxDegreeOfParallelism = 4 }, (i, loopState) =>
			{
				// create a new request body with the correct registration number
				Dictionary<string, string> requestBody = new Dictionary<string, string>();
				foreach (KeyValuePair<string, string> keyValuePair in formParameters)
				{
					if (!keyValuePair.Key.Contains("registrationNumber"))
						requestBody.Add(keyValuePair.Key, keyValuePair.Value);
					else
						requestBody.Add(keyValuePair.Key, i.ToString().Trim());
				}

				// do POST
				try
				{
					HttpResponseMessage postResponse = httpClient.PostAsync(targetUrl, new FormUrlEncodedContent(requestBody)).Result;
					if (!postResponse.IsSuccessStatusCode)
					{
						failedRegNumbers.Add(i);
						if (postResponse.StatusCode == HttpStatusCode.TooManyRequests)
						{
							loopState.Stop(); // no use continuing any thread when rate-limited
							return; // skips this iteration. just doing .Stop() alone doesn't skip the code below it
						}

					}
					else
					{
						string postResponseBody = postResponse.Content.ReadAsStringAsync().Result;

						HtmlDocument companyDetailsResponseHtml = new HtmlDocument();
						companyDetailsResponseHtml.LoadHtml(postResponseBody);

						HtmlNodeCollection companyPropertyNodes = companyDetailsResponseHtml.DocumentNode.SelectNodes("//td[contains(@id, 'companyDetails')]");

						if (companyPropertyNodes != null && companyPropertyNodes.Count > 0)
						{
							// extract all companies' details for this reg. no. and populate the concurrent dictionary
							// weirdly enough, for a single reg. no., there can be multiple companies - hence multiple rows
							List<string[]> companies = new List<string[]>();
							string[] companyProperties = new string[8];
							int counter = 0;
							foreach (HtmlNode companyPropertyNode in companyPropertyNodes)
							{
								companyProperties[counter++] = HttpUtility.HtmlDecode(companyPropertyNode.InnerText);
								if (counter == 8) // one row worth of data completed
								{
									companies.Add(companyProperties); // add this row to the list
									companyProperties = new string[8]; // create a new array for the next row
									counter = 0; // reset the counter
								}
							}

							if (companyPropertyNodes.Count > 0)
							{
								if (i == int.Parse(companies[0][3]))
									harvestedCompanies.TryAdd(i, companies);
								else
									failedRegNumbers.Add(i);
							}

						}



					}
				}
				catch (Exception ex)
				{
					failedRegNumbers.Add(i);
				}
			});
#endif

#if SEQUENTIAL
			for(int i = 1; i < 1000000; i++)
			{
				// create a new request body with the correct registration number
				Dictionary<string, string> requestBody = new Dictionary<string, string>();
				foreach (KeyValuePair<string, string> keyValuePair in formParameters)
				{
					if (!keyValuePair.Key.Contains("registrationNumber"))
						requestBody.Add(keyValuePair.Key, keyValuePair.Value);
					else
						requestBody.Add(keyValuePair.Key, i.ToString().Trim());
				}



				// do POST
				try
				{
					HttpResponseMessage postResponse = httpClient.PostAsync(targetUrl, new FormUrlEncodedContent(requestBody)).Result;
					if (!postResponse.IsSuccessStatusCode)
					{
						failedRegNumbers.Add(i);
						if (postResponse.StatusCode == HttpStatusCode.TooManyRequests)
						{
							break;
						}

					}
					else
					{
						string postResponseBody = postResponse.Content.ReadAsStringAsync().Result;

						HtmlDocument companyDetailsResponseHtml = new HtmlDocument();
						companyDetailsResponseHtml.LoadHtml(postResponseBody);

						HtmlNodeCollection companyPropertyNodes = companyDetailsResponseHtml.DocumentNode.SelectNodes("//td[contains(@id, 'companyDetails')]");

						// if there's no entry for this reg. no., prolly means we've reached the last reg. no.
						if (companyPropertyNodes == null || companyPropertyNodes.Count == 0)
						{
							break;
						}


						// extract all companies' details for this reg. no. and populate the concurrent dictionary
						// weirdly enough, for a single reg. no., there can be multiple companies - hence multiple rows
						List<string[]> companies = new List<string[]>();
						string[] companyProperties = new string[8];
						int counter = 0;
						foreach (HtmlNode companyPropertyNode in companyPropertyNodes)
						{
							companyProperties[counter++] = HttpUtility.HtmlDecode(companyPropertyNode.InnerText);
							if (counter == 8) // one row worth of data completed
							{
								companies.Add(companyProperties); // add this row to the list
								companyProperties = new string[8]; // create a new array for the next row
								counter = 0; // reset the counter
							}
						}
						harvestedCompanies.TryAdd(i, companies);

					}
				}
				catch (Exception ex)
				{
					failedRegNumbers.Add(i);
				}
			}
#endif

#if TASK_WHENALL
			

			using (SemaphoreSlim semaphore = new SemaphoreSlim(10_000)) // allow at most 1000 concurrent tasks
			{
				IEnumerable<Task> harvestTasks = Enumerable.Range(0, 1_000_000).Select(async regNum =>
				{
					await semaphore.WaitAsync(); // block if too many tasks have entered the following block

					try
					{
						await HarvestCompanies(regNum, failedRegNumbers, processedRegNumbers, harvestedCompanies);
					}
					catch (Exception ex)
					{
						// if exception occurred during harvesting, add this reg number to the failed list
						failedRegNumbers.Add(regNum);

					}
					finally
					{
						semaphore.Release(); // signal that we've exited the HTTP request block for this reg number
					}

				});

				ManualResetEventSlim killSwitch = new ManualResetEventSlim();
				Task.Run(async () =>
				{
					while (!killSwitch.IsSet)
					{
						Console.WriteLine($"Processed regNums : {processedRegNumbers.Count}");
						Console.WriteLine($"Harvested companies : {harvestedCompanies.Count}");
						Console.WriteLine($"Failed regNums: {failedRegNumbers.Count}");
						Console.WriteLine($"Queue fill rate: {dbStore.QueueFillRate:F3}");
						Console.WriteLine($"Queue drain rate: {dbStore.QueueDrainRate:F3}");
						await Task.Delay(10000);
					}

				});

				Task.WhenAll(harvestTasks).Wait();
				killSwitch.Set();
			}

			stopwatch.Stop();
			TimeSpan elapsedTime = stopwatch.Elapsed;


			SaveCompaniesToFile(harvestedCompanies, failedRegNumbers);
			Console.WriteLine($"Finished scraping in {elapsedTime.TotalSeconds} seconds");
			
#endif


#if TASK_WHENALL_WITH_DBSTORE
			ManualResetEventSlim killSwitch = new ManualResetEventSlim();
			using (DBStore dbStore = new DBStore("output.db", null, 5, 1000, 1000, DBStore.FlushRateControlMethod.SimpleSleep))
			{
				using (SemaphoreSlim semaphore = new SemaphoreSlim(1000)) // allow at most 1000 concurrent tasks
				{
					IEnumerable<Task> harvestTasks = Enumerable.Range(0, 1_000_000).Select(async regNum =>
					{
						//await semaphore.WaitAsync(); // block if too many tasks have entered the following block

						try
						{
							await HarvestCompanies(regNum, failedRegNumbers, processedRegNumbers, null, dbStore);
						}
						catch (Exception ex)
						{
							// if exception occurred during harvesting, add this reg number to the failed list
							failedRegNumbers.Add(regNum);

						}
						finally
						{
							//semaphore.Release(); // signal that we've exited the HTTP request block for this reg number
						}

					});


					Task.Run(async () =>
					{
						while (!killSwitch.IsSet)
						{
							Console.WriteLine($"Processed regNums : {processedRegNumbers.Count}");
							Console.WriteLine($"Harvested companies : {dbStore.TotalEntriesAdded}");
							Console.WriteLine($"Failed regNums: {failedRegNumbers.Count}");
							Console.WriteLine($"Queue size: {dbStore.QueueSize}");
							Console.WriteLine($"Queue fill rate: {dbStore.QueueFillRate:F3}");
							Console.WriteLine($"Queue drain rate: {dbStore.QueueDrainRate:F3}");
							Console.WriteLine($"Sleep between flushes: {dbStore.SleepDurationMsBetweenFlushesToDb}\n");
							await Task.Delay(1000);
						}

					});

					Task.WhenAll(harvestTasks).Wait();

				}
			}

			killSwitch.Set();
			stopwatch.Stop();
			TimeSpan elapsedTime = stopwatch.Elapsed;
			Console.WriteLine($"Finished scraping in {elapsedTime.TotalSeconds} seconds");
			SaveFailedRegNumsToFile(failedRegNumbers);
#endif

#if PARALLEL_FOR_ASYNC
			ManualResetEventSlim killSwitch = new ManualResetEventSlim();
			Task parallelForTask;
			DBStore dbStore = new DBStore("output.db", null, 5, 1000, 1000, DBStore.FlushRateControlMethod.SimpleSleep);

			Task.Run(async () =>
			{
				while (!killSwitch.IsSet)
				{
					Console.WriteLine($"Processed regNums : {processedRegNumbers.Count}");
					Console.WriteLine($"Harvested companies : {dbStore.TotalEntriesAdded}");
					Console.WriteLine($"Failed regNums: {failedRegNumbers.Count}");
					Console.WriteLine($"Queue size: {dbStore.QueueSize}");
					Console.WriteLine($"Queue fill rate: {dbStore.QueueFillRate:F3}");
					Console.WriteLine($"Queue drain rate: {dbStore.QueueDrainRate:F3}");
					Console.WriteLine($"Sleep between flushes: {dbStore.SleepDurationMsBetweenFlushesToDb}\n");
					await Task.Delay(1000);
				}

			});

			parallelForTask = Parallel.ForAsync(1, 1_000_000, new ParallelOptions { MaxDegreeOfParallelism = 1000 }, async (i, _) =>
			{
				try
				{
					await HarvestCompanies(i, failedRegNumbers, processedRegNumbers, null, dbStore);
				}
				catch (Exception ex)
				{
					failedRegNumbers.Add(i);
				}
			});


			parallelForTask.Wait();
			dbStore.Dispose();
			killSwitch.Set();
			stopwatch.Stop();
			TimeSpan elapsedTime = stopwatch.Elapsed;
			Console.WriteLine($"Finished scraping in {elapsedTime.TotalSeconds} seconds");
			SaveFailedRegNumsToFile(failedRegNumbers);
#endif

		}

		/// <summary>
		/// GETs a valid jsession cookie from the targetUrl, then POSTs the given registration number to check if a company exists for it. Then updates the given variables with failedRegNumbers and processedRegNumbers.
		/// The harvestedCompanies ConcurrentDictionary, if provided, is filled with successfully verified companies for the given regNumToCheck; as is the DB pointed to by dbStore.
		/// </summary>
		/// <param name="regNumToCheck">The registration number to probe</param>
		/// <param name="failedRegNumbers">Collection of registration numbers for which probing failed for a variety of reasons. Updated in place by the method.</param>
		/// <param name="processedRegNumbers">Collection that holds all the registration numbers that have been probed. Updated in place by the method.</param>
		/// <param name="harvestedCompanies">Dictionary that holds all the companies extracted for each registration number. Updated in place by the method.</param>
		/// <param name="dbStore">A DBStore object that handles writes of successfully harvested companies to DB</param>
		/// <returns></returns>
		/// <exception cref="Exception"></exception>
		private static async Task HarvestCompanies(int regNumToCheck, ConcurrentBag<int> failedRegNumbers, ConcurrentBag<int> processedRegNumbers, ConcurrentDictionary<int, List<string[]>>? harvestedCompanies, DBStore? dbStore)
		{
			// update book-keeping collection
			processedRegNumbers.Add(regNumToCheck);

			// get a fresh HttpClient instance with a fresh cookie, and the relevant form parameters
			(HttpClient httpClient, Dictionary<string, string> formParameters) = await GetHttpClientWithFreshCookieAndFormParams() ?? default;

			if (httpClient == null || formParameters == null)
				throw new Exception("Couldn't instantiate HttpClient or obtain form parameters");

			// create a new request body with the correct registration number
			Dictionary<string, string> requestBody = new Dictionary<string, string>();
			foreach (KeyValuePair<string, string> keyValuePair in formParameters)
			{
				if (!keyValuePair.Key.Contains("registrationNumber"))
					requestBody.Add(keyValuePair.Key, keyValuePair.Value);
				else
					requestBody.Add(keyValuePair.Key, regNumToCheck.ToString().Trim());
			}

			// make POST request and extract all corresponding company details for this reg number
			int runCount = 0, maxRuns = 3, initialBackoffTime = 1000;
			while (runCount < maxRuns)
			{
				runCount++;
				try
				{
					HttpResponseMessage postResponse = await httpClient.PostAsync(targetUrl, new FormUrlEncodedContent(requestBody));
					if (!postResponse.IsSuccessStatusCode)
					{
						if (postResponse.StatusCode == HttpStatusCode.TooManyRequests)
						{
							await Task.Delay(initialBackoffTime); // back off for a while
							initialBackoffTime *= 2; // exponential back-off
						}
						if (runCount == maxRuns)
							failedRegNumbers.Add(regNumToCheck); // add to failed list if max retries exhausted
					}
					else
					{
						string postResponseBody = await postResponse.Content.ReadAsStringAsync();

						HtmlDocument companyDetailsResponseHtml = new HtmlDocument();
						companyDetailsResponseHtml.LoadHtml(postResponseBody);

						HtmlNodeCollection companyPropertyNodes = companyDetailsResponseHtml.DocumentNode.SelectNodes("//td[contains(@id, 'companyDetails')]");

						if (companyPropertyNodes != null && companyPropertyNodes.Count > 0)
						{
							// extract all companies' details for this reg. no. and populate the concurrent dictionary
							// weirdly enough, for a single reg. no., there can be multiple companies - hence multiple rows
							List<string[]> companies = new List<string[]>();
							string[] companyProperties = new string[8];
							int counter = 0;
							foreach (HtmlNode companyPropertyNode in companyPropertyNodes)
							{
								companyProperties[counter++] = HttpUtility.HtmlDecode(companyPropertyNode.InnerText);
								if (counter == 8) // one row worth of data completed
								{
									companies.Add(companyProperties); // add this row to the list
									companyProperties = new string[8]; // create a new array for the next row
									counter = 0; // reset the counter
								}
							}

							if (companies.Count > 0)
							{
								if (regNumToCheck == int.Parse(companies[0][3])) // if the companies have the right reg number
								{
									harvestedCompanies?.TryAdd(regNumToCheck, companies);
									if (dbStore != null)
									{
										foreach (string[] companyDetails in companies)
										{
											dbStore?.AddCompany(new Company
											{
												NepaliName = companyDetails[1],
												EnglishName = companyDetails[2],
												RegistrationNumber = int.Parse(companyDetails[3]),
												RegistrationDateBS = companyDetails[4],
												CompanyType = companyDetails[5],
												Address = companyDetails[6],
												LastContactDateBS = companyDetails[7]
											});
										}
									}
									break; // break out of this retry while loop
								}
								else
								{
									if (runCount == maxRuns)
										failedRegNumbers.Add(regNumToCheck); // add to failed list if max retries exhausted
								}
							}


						}
						else
							break; // apparently, there's no company belonging to this reg number, so no need to retry, hence break out
					}
				}
				catch (Exception ex)
				{
					await Task.Delay(initialBackoffTime); // back off for a while
					initialBackoffTime *= 2; // exponential back-off

					if (runCount == maxRuns)
						failedRegNumbers.Add(regNumToCheck); // add to failed list if max retries exhausted
				}


			}

		}

		/// <summary>
		/// GETs the targetUrl for a valid jsession cookie and returns a freshly minted HttpClient with the cookie header set, along with a Dictionary of all the necessary form parameters (hidden and otherwise) for the subsequent POST request needed to probe a registration number
		/// </summary>
		/// <returns></returns>
		private static async Task<(HttpClient, Dictionary<string, string>)?> GetHttpClientWithFreshCookieAndFormParams()
		{
			HttpClient httpClient = new HttpClient();

			HttpResponseMessage firstResponse = await httpClient.GetAsync(targetUrl);
			if (!firstResponse.IsSuccessStatusCode)
			{
				Console.WriteLine("First response failed!");
				return null;
			}

			HttpResponseHeaders responseHeaders = firstResponse.Headers;
			IEnumerable<string>? cookies;
			if (!responseHeaders.TryGetValues("Set-Cookie", out cookies))
			{
				Console.WriteLine("Cookie not obtained from first response!");
				return null;
			}
			string cookieValue = cookies.First();
			cookieValue = cookieValue.Split(';', StringSplitOptions.RemoveEmptyEntries)[0]; // keep only the actual cookie, not the metadata

			HttpContent responseContent = firstResponse.Content;
			string responseBody = await responseContent.ReadAsStringAsync(); // actual response body

			HtmlDocument htmlDocument = new HtmlDocument();
			htmlDocument.LoadHtml(responseBody); // parse the HTML text to an HtmlDocument

			// retrieve the form input name-value pairs from the HtmlDocument and load them to a dictionary
			HtmlNodeCollection formInputNodes = htmlDocument.DocumentNode.SelectNodes(@"//input[@type='hidden' or @type='text' or @type='submit']");
			Dictionary<string, string> formParameters = new Dictionary<string, string>();
			foreach (HtmlNode hiddenInputNode in formInputNodes)
			{
				string name = hiddenInputNode.Attributes["name"].Value;
				string value = hiddenInputNode.Attributes["value"].Value;
				formParameters.Add(name, value);
			}

			// add cookie to request header now that we're going to POST
			httpClient.DefaultRequestHeaders.Add("Cookie", cookieValue);

			return (httpClient, formParameters);
		}

		/// <summary>
		/// Dumps the given dictionary of registration number : list of array of company properties to a CSV file
		/// </summary>
		/// <param name="harvestedCompanies">The collection of harvested companies</param>
		private static void SaveCompaniesToFile(ConcurrentDictionary<int, List<string[]>> harvestedCompanies)
		{
			using (StreamWriter writer = new StreamWriter("completed-companies.csv"))
			{
				writer.WriteLine("SN,NepaliName,EnglishName,RegNo,RegdDateBS,CompanyType,Address,LastContactedBS");
				foreach (KeyValuePair<int, List<string[]>> companyInfos in harvestedCompanies)
				{
					foreach (string[] companyInfo in companyInfos.Value)
					{
						writer.WriteLine($"{companyInfo[0]};;||;;{companyInfo[1]};;||;;{companyInfo[2]};;||;;{companyInfo[3]};;||;;{companyInfo[4]};;||;;{companyInfo[5]};;||;;{companyInfo[6]};;||;;{companyInfo[7]}");
					}
				}
			}
		}

		/// <summary>
		/// Dumps the given collection of registration numbers for which probing failed to a text file, one number on each line
		/// </summary>
		/// <param name="failedCompanyRegNums">Collection of failed registration numbers</param>
		private static void SaveFailedRegNumsToFile(ConcurrentBag<int> failedCompanyRegNums)
		{
			using (StreamWriter writer = new StreamWriter("failed-regnos.txt"))
			{
				foreach (int regNo in failedCompanyRegNums)
				{
					writer.WriteLine(regNo);
				}

			}
		}
	}
}

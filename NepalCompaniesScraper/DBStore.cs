using Microsoft.Data.Sqlite;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NepalCompaniesScraper
{
	/*
	 * It's astounding that 80% of the complexity in this class is down to queue-related metrics, which is needed for the Dynamic Rate method of flushing the queue to DB and for user-reporting (hardly 20% of the functionality)
	 * The Dynamic Rate method isn't any better than the other two methods either, and mostly leads to very frequent flushes
	 * The Exponential Backoff method is very rapid when a lot of items are being added to the queue but very quiet otherwise (which happens towards the end of the harvest when companies are hard to find)
	 * The humble Simple Sleep method is pretty balanced
	 */
	internal class DBStore : IDisposable
	{
		#region Property-backed constructor-parameter fields
		private string _dbPath;
		private ILogger? _logger;
		private int _maxDeqTries; // how many times to try dequeue before giving up
		private int _transactionBatchSize; // upper limit of batch size for DB insert
		private int _maxSleepDurationMsBetweenFlushToDb; // upper threshold of sleep duration between consecutive data flushes to DB
		private FlushRateControlMethod _flushRateControlMethod; //  method for sleeping between consecutive flushes
		#endregion

		#region Internal fields
		private ConcurrentQueue<Company> companies;
		private BackgroundWorker offloaderThread; // background thread tasked with dequeuing companies from the queue to DB
		private double companyQueueFillRate; // millisecond frequency i.e. how many companies queued up per ms
		private DateTime lastQueueCheckTime; // records the instant the queue was checked (for fill rate calculation)
		private DateTime lastQueueFlushedDateTime; // records the last time a batch of companies was flushed to DB. no guarantee on the size of the batch except that it is < _transactionBatchSize
		private double companyQueueDrainRate; // millisecond frequency for companies removal from the queue
		private int lastFlushBatchSize; // number of companies flushed from the queue to DB last time
		private int lastTotalCompaniesAdded; // cumulative number of companies added the last time it was checked
		private int sleepDurationMsBetweenFlushesToDb;
		private int totalEntriesAdded;
		private BackgroundWorker metricsThread; // background thread tasked with maintaining a current queue drain and fill rate
		private AutoResetEvent offloaderThreadSleepSignal; // controls sleep period between consecutive flushes to DB by the offloader thread
		private DateTime lastTimeAddedToQueue; // records the instant a new element was added to the queue (for exponential back-off sleep between flushes)
		private ManualResetEvent metricsThreadDeadSignal; // set to true by metrics worker thread when about to exit. required coz backgroundworker threads are killed on program exit.
		private ManualResetEvent offloaderThreadDeadSignal; //set to true by offloader worker thread when about to exit. required coz backgroundworker threads are killed on program exit.
		#endregion

		#region Properties
		internal ILogger? Logger { get => _logger; set => _logger = value; }
		public int MaxDeqTries { get => _maxDeqTries; set => _maxDeqTries = value; }
		public int TransactionBatchSize { get => _transactionBatchSize; set => _transactionBatchSize = value; }
		public int MaxSleepDurationMsBetweenFlushToDb { get => _maxSleepDurationMsBetweenFlushToDb; set => _maxSleepDurationMsBetweenFlushToDb = value; }
		public string DbPath { get => _dbPath; set => _dbPath = value; }
		public double QueueFillRate { get => companyQueueFillRate; }
		public double QueueDrainRate { get => companyQueueDrainRate; }
		public int SleepDurationMsBetweenFlushesToDb { get => sleepDurationMsBetweenFlushesToDb; }
		public int TotalEntriesAdded { get => totalEntriesAdded; }
		public int QueueSize { get => companies.Count; }
		public int LastFlushBatchSize { get => LastFlushBatchSize; }
		public FlushRateControlMethod Method { get => _flushRateControlMethod; set => _flushRateControlMethod = value; }
		#endregion
		public enum FlushRateControlMethod
		{
			SimpleSleep,
			ExponentialBackOff,
			DynamicRate
		}

		/// <summary>
		/// Construct an object of DBStore class ready to write to DB.
		/// </summary>
		/// <param name="dbPath">Path to the database</param>
		/// <param name="logger">Logger</param>
		/// <param name="maxDequeueTries">How many attempts to make to dequeue items from the queue before giving up</param>
		/// <param name="transactionBatchSize">How many items to include in a batch to write to DB</param>
		/// <param name="maxSleepDurationMsBetweenFlushToDb">How many milliseconds to wait before consecutive batch formation and flushing to DB. Only used for SimpleSleep and DynamicRate methods</param>
		/// <param name="method">Method used to wait between consecutive flushes</param>
		public DBStore(string dbPath, ILogger? logger = null, int maxDequeueTries = 5, int transactionBatchSize = 1000, int maxSleepDurationMsBetweenFlushToDb = 1000, FlushRateControlMethod method = FlushRateControlMethod.SimpleSleep)
		{
			this._dbPath = dbPath;
			this._logger = logger;
			this._maxDeqTries = maxDequeueTries;
			this._transactionBatchSize = transactionBatchSize;
			this._maxSleepDurationMsBetweenFlushToDb = maxSleepDurationMsBetweenFlushToDb;
			this._flushRateControlMethod = method;

			this.companies = new ConcurrentQueue<Company>();
			this.offloaderThreadSleepSignal = new AutoResetEvent(false);
			this.metricsThreadDeadSignal = new ManualResetEvent(false);
			this.offloaderThreadDeadSignal = new ManualResetEvent(false);
			this.EnsureDBExists();
			this.StartMonitor();
		}

		/// <summary>
		/// Add the specified company to DB
		/// </summary>
		/// <param name="company"></param>
		public void AddCompany(Company company)
		{
			this.companies.Enqueue(company);
			this.lastTimeAddedToQueue = DateTime.Now;
			this.totalEntriesAdded++;
		}

		/// <summary>
		/// Creates tables in the specified DB file path if they don't exist
		/// </summary>
		/// <returns></returns>
		private bool EnsureDBExists()
		{
			bool retval = false;

			try
			{
				using (SqliteConnection connection = new SqliteConnection($"Data Source='{this._dbPath}'"))
				{
					connection.Open();
					SqliteCommand command = connection.CreateCommand();
					command.CommandText = @"CREATE TABLE IF NOT EXISTS companies (id INTEGER PRIMARY KEY AUTOINCREMENT, RegNum INTEGER, NepaliName TEXT, EnglishName TEXT, 
						RegdDate TEXT, CompanyType TEXT, Address TEXT, LastContactDate TEXT);";
					command.ExecuteNonQuery();
					retval = true;
				}
			}
			catch (SqliteException ex)
			{
				this._logger?.LogException(ex);
			}


			return retval;
		}

		/// <summary>
		/// Start the background thread that offloads the company queue to DB on disk periodically, and the background thread that maintains queue metrics
		/// </summary>
		private void StartMonitor()
		{
			if (offloaderThread != null && offloaderThread.WorkerSupportsCancellation)
				offloaderThread.CancelAsync();
			offloaderThread = new BackgroundWorker() { WorkerSupportsCancellation = true };
			offloaderThread.DoWork += OffloaderThread_DoWork;
			offloaderThread.RunWorkerAsync();


			if (metricsThread != null && metricsThread.WorkerSupportsCancellation)
				metricsThread.CancelAsync();
			metricsThread = new BackgroundWorker() { WorkerSupportsCancellation = true };
			metricsThread.DoWork += MetricsThread_DoWork;
			metricsThread.RunWorkerAsync();
		}

		/// <summary>
		/// Constantly updates the queue-related metrics essential for the Dynamic Rate method and for user-reporting
		/// </summary>
		/// <param name="sender"></param>
		/// <param name="e"></param>
		private void MetricsThread_DoWork(object? sender, DoWorkEventArgs e)
		{
			if (sender is BackgroundWorker bgWorker)
			{
				while (!bgWorker.CancellationPending)
				{
					// calculate queue fill rate
					this.companyQueueFillRate = (this.totalEntriesAdded - this.lastTotalCompaniesAdded) / (DateTime.Now - this.lastQueueCheckTime).TotalMilliseconds;
					this.lastTotalCompaniesAdded = this.totalEntriesAdded;
					this.lastQueueCheckTime = DateTime.Now;

					// calculate sleep duration between consecutive queue->DB flushes
					switch (this._flushRateControlMethod)
					{
						case FlushRateControlMethod.DynamicRate:
							// calculate sleep duration for offloader thread between consecutive flushes (for dynamic rate method only)
							// sleep for 1/(fill rate - drain rate) ms, with normalization for div. by 0 and -ve cases
							double sleepDuration;
							if (this.companyQueueDrainRate >= this.companyQueueFillRate)
							{
								if (this.companyQueueDrainRate == 0)
									sleepDuration = this._maxSleepDurationMsBetweenFlushToDb;
								else
									sleepDuration = 1 / this.companyQueueDrainRate;
							}
							else
								sleepDuration = 1 / (this.companyQueueFillRate - this.companyQueueDrainRate);
							this.sleepDurationMsBetweenFlushesToDb = (int)(sleepDuration > this._maxSleepDurationMsBetweenFlushToDb ? this._maxSleepDurationMsBetweenFlushToDb : sleepDuration);
							break;
						case FlushRateControlMethod.ExponentialBackOff:
							this.sleepDurationMsBetweenFlushesToDb = (DateTime.Now - this.lastTimeAddedToQueue).Milliseconds;
							break;
						case FlushRateControlMethod.SimpleSleep:
							this.sleepDurationMsBetweenFlushesToDb = this._maxSleepDurationMsBetweenFlushToDb;
							break;
					}

					Thread.Sleep(1000);
				}
				this.metricsThreadDeadSignal.Set(); // signal that this thread is done
			}
		}

		/// <summary>
		/// The workhorse of the class that takes the appropriate number of elements from the queue and writes them to the DB, at the right times.
		/// </summary>
		/// <param name="sender"></param>
		/// <param name="e"></param>
		private void OffloaderThread_DoWork(object? sender, DoWorkEventArgs e)
		{
			if (sender is BackgroundWorker bgWorker)
			{
				using (SqliteConnection connection = new SqliteConnection($"Data Source='{this._dbPath}'"))
				{
					connection.Open();
					while (!bgWorker.CancellationPending || !companies.IsEmpty)
					{
						// form a batch of companies and flush to DB
						List<Company> companiesBatchForTransaction = new List<Company>();
						while (!this.companies.IsEmpty && companiesBatchForTransaction.Count < TransactionBatchSize && Dequeue(out Company company, this._maxDeqTries)) // order of conditions is important
						{
							companiesBatchForTransaction.Add(company);
						}
						if (companiesBatchForTransaction.Count > 0)
						{
							SaveCompaniesToDB(connection, companiesBatchForTransaction);

							// update queue drain rate metric
							this.companyQueueDrainRate = companiesBatchForTransaction.Count / (DateTime.Now - this.lastQueueFlushedDateTime).TotalMilliseconds;
							this.lastQueueFlushedDateTime = DateTime.Now;
						}
						this.lastFlushBatchSize = companiesBatchForTransaction.Count;

						this.offloaderThreadSleepSignal.WaitOne(this.sleepDurationMsBetweenFlushesToDb); // sleep
					}
				}
				this.offloaderThreadDeadSignal.Set(); // signal that this thread is done

			}

		}

		/// <summary>
		/// Writes the provided list of companies to the DB corresponding to the provided connection
		/// </summary>
		/// <param name="openConnection"></param>
		/// <param name="companies"></param>
		private void SaveCompaniesToDB(SqliteConnection openConnection, List<Company> companies)
		{
			SqliteTransaction transaction = openConnection.BeginTransaction();
			SqliteCommand command = openConnection.CreateCommand();
			command.CommandText = @"INSERT INTO companies (RegNum, NepaliName, EnglishName, RegdDate, CompanyType, Address, LastContactDate)
					VALUES ($regNum, $nepaliName, $englishName, $regdDate, $companyType, $address, $lastContactDate)";
			foreach (Company company in companies)
			{
				command.Parameters.Clear();
				command.Parameters.AddWithValue("$regNum", company.RegistrationNumber);
				command.Parameters.AddWithValue("$nepaliName", company.NepaliName);
				command.Parameters.AddWithValue("$englishName", company.EnglishName);
				command.Parameters.AddWithValue("$regdDate", company.RegistrationDateBS);
				command.Parameters.AddWithValue("$companyType", company.CompanyType);
				command.Parameters.AddWithValue("$address", company.Address);
				command.Parameters.AddWithValue("$lastContactDate", company.LastContactDateBS);
				command.ExecuteNonQuery();
			}
			transaction.Commit();
		}

		/// <summary>
		/// Dequeues a company from the ConcurrentQueue with a user-specified number of retries when dequeuing fails
		/// </summary>
		/// <param name="company">Dequeued company</param>
		/// <param name="maxDequeueTries">Maximum number of dequeue attempts before giving up</param>
		/// <returns>True if dequeuing was successful</returns>
		private bool Dequeue(out Company company, int maxDequeueTries)
		{
			int deqTries = 0;
			bool deqSuccess;
			do
			{
				deqSuccess = this.companies.TryDequeue(out company);
				deqTries++;
			} while (!deqSuccess && deqTries <= maxDequeueTries);
			return deqSuccess;
		}

		/// <summary>
		/// Required for cleanly finishing writing to DB. Background worker threads just die when the program ends, so this is required for DB integrity.
		/// </summary>
		public void Dispose()
		{
			this.metricsThread.CancelAsync(); // stop updating the sleep duration
			this.metricsThreadDeadSignal.WaitOne(2000); // wait a max. of 2 seconds for the metrics thread to die. this is essential before updating sleepDurationMsBetweenFlushesToDb

			this.sleepDurationMsBetweenFlushesToDb = 0; // give the offloader thread no rest, we need to finish fast

			this.offloaderThreadSleepSignal.Set(); // wake up the offloader thread right away

			offloaderThread.CancelAsync(); // tell the offloader thread to stop when finished offloading whole queue

			this.offloaderThreadDeadSignal.WaitOne(5000); // give a max of 5s as grace period for offloading. after that, all bets are off as far as the DB's integrity.

		}
	}
}

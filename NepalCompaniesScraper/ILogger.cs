using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NepalCompaniesScraper
{
	internal interface ILogger
	{
		void LogMsg(string message);
		void LogException(Exception exception);
	}
}

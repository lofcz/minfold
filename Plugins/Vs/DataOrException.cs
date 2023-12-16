using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MinfoldVs
{
	internal class DataOrException<T>
	{
		public T? Data { get; set; }
		public Exception? Exception { get; set; }

		public DataOrException(T data)
		{
			Data = data;
		}

		public DataOrException(Exception exception)
		{
			Exception = exception;
		}
	}

	internal class StdOutErr
	{
		public List<string> StdOut { get; set; }
		public List<string> StdErr { get; set; }
	}
}

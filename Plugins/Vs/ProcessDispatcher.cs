using Microsoft.VisualStudio.Threading;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace MinfoldVs
{
	internal static class ProcessDispatcher
	{
		public static async Task<DataOrException<bool>> Available(string cmd)
		{
			DataOrException<List<string>> data = await Run(RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? "which" : "where", cmd);

			if (data.Exception is not null)
			{
				return new DataOrException<bool>(data.Exception);
			}

			if (data.Data is null)
			{
				return new DataOrException<bool>(false);
			}

			foreach (string x in data.Data)
			{
				if (x.Trim().Replace("\n", string.Empty).Replace("\r", "").Length > 0)
				{
					return new DataOrException<bool>(true);
				}
			}

			return new DataOrException<bool>(false);
		}

		public static async Task<DataOrException<List<string>>> Run(string command, string args)
		{
			List<string> outputLines = [];

			ProcessStartInfo processStartInfo = new ProcessStartInfo
			{
				CreateNoWindow = true,
				RedirectStandardOutput = true,
				RedirectStandardInput = true,
				UseShellExecute = false,
				Arguments = args,
				FileName = command
			};

			using Process process = new Process();
			process.StartInfo = processStartInfo;
			process.EnableRaisingEvents = true;

			process.OutputDataReceived += new DataReceivedEventHandler
			(
				delegate (object pSender, DataReceivedEventArgs pE)
				{
					outputLines.Add($"{pE.Data}\n");
				}
			);

			try
			{
				process.Start();
				process.BeginOutputReadLine();
				await process.WaitForExitAsync();
				process.CancelOutputRead();
				return new DataOrException<List<string>>(outputLines);
			}
			catch (Exception processException)
			{
				return new DataOrException<List<string>>(processException);
			}
		}
	}
}

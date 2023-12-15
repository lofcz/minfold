using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Media;
using System.Threading.Tasks;
using Microsoft.VisualStudio.Threading;
using System.Collections.Generic;

namespace MinfoldVs
{
	/// <summary>
	/// Interaction logic for MinfoldToolControl.
	/// </summary>
	public partial class MinfoldToolControl : UserControl
	{
		public static readonly DependencyProperty TextDependency = DependencyProperty.Register(nameof(MinfoldText), typeof(string), typeof(MinfoldToolControl), new UIPropertyMetadata(null));

		public string MinfoldText
		{
			get { return (string)GetValue(TextDependency); }
			set
			{
				SetValue(TextDependency, value);
			}
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="MinfoldToolControl"/> class.
		/// </summary>
		public MinfoldToolControl()
		{
			this.InitializeComponent();
			MinfoldText = string.Empty;
		}

		private void TextBox_TextChanged(object sender, TextChangedEventArgs e)
		{

        }

		private void RichTextBox_TextChanged(object sender, TextChangedEventArgs e)
		{

		}

		void SetIoSize()
		{
			ioScroll.Height = Math.Abs(minfoldGrid.ActualHeight - inputPanel.ActualHeight - 16);
		}

		private async void Button_Click(object sender, RoutedEventArgs e)
		{
			SetIoSize();

			List<string> outputLines = new List<string>();

			ProcessStartInfo processStartInfo = new ProcessStartInfo
			{
				CreateNoWindow = true,
				RedirectStandardOutput = true,
				RedirectStandardInput = true,
				UseShellExecute = false,
				Arguments = "--help",
				FileName = "minfold"
			};

			Process process = new Process();
			process.StartInfo = processStartInfo;
			process.EnableRaisingEvents = true;

			process.OutputDataReceived += new DataReceivedEventHandler
			(
				delegate (object pSender, DataReceivedEventArgs pE)
				{
					outputLines.Add($"{pE.Data}\n");

					/*Dispatcher.Invoke(() =>
					{
						ioText.Inlines.Add(new Run($"{pE.Data}\n"));
					});*/
				}
			);

			ioText.Inlines.Clear();

			try
			{
				process.Start();
				process.BeginOutputReadLine();
				await process.WaitForExitAsync();
				process.CancelOutputRead();
			}
			catch (Exception processException)
			{

			}

			foreach (var x in outputLines)
			{
				ioText.Inlines.Add(new Run(x));
			}

			ioText.Inlines.Add(new Run("muij string 2\n") { Foreground = Brushes.Blue });
			ioText.Inlines.Add(new Bold(new Run("muij string 2\n") { Foreground = Brushes.Blue }));
		}

		private void UserControl_SizeChanged(object sender, SizeChangedEventArgs e)
		{
			SetIoSize();
		}
	}
}
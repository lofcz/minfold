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
using static Microsoft.VisualStudio.Threading.AsyncReaderWriterLock;
using System.Windows.Markup;

namespace MinfoldVs
{
	/// <summary>
	/// Interaction logic for MinfoldToolControl.
	/// </summary>
	public partial class MinfoldToolControl : UserControl
	{
		public static readonly DependencyProperty TextDependency = DependencyProperty.Register(nameof(MinfoldText), typeof(string), typeof(MinfoldToolControl), new UIPropertyMetadata(null));
		private bool? minfoldAvailable = null;
		private static readonly SolidColorBrush errorBrush = new SolidColorBrush(Color.FromArgb(255, 255, 65, 54));
		private static readonly SolidColorBrush warningBrush = new SolidColorBrush(Color.FromArgb(255, 255, 133, 27));
		private bool busy;

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

		void RenderException(Exception e, string text)
		{
			ioText.Inlines.Clear();
			ioText.Inlines.Add(new Run($"{text}{Environment.NewLine}") { Foreground = errorBrush });
			ioText.Inlines.Add(new Run($"Native exception:{Environment.NewLine}") { Foreground = errorBrush });
			ioText.Inlines.Add(new Run(e.Message) { Foreground = errorBrush });
			ioText.InvalidateVisual();
		}

		void RenderError(string text)
		{
			ioText.Inlines.Clear();
			ioText.Inlines.Add(new Run($"{text}") { Foreground = errorBrush });
			ioText.InvalidateVisual();
		}

		void RenderWarning(string text)
		{
			ioText.Inlines.Clear();
			ioText.Inlines.Add(new Run($"{text}") { Foreground = warningBrush });
			ioText.InvalidateVisual();
		}

		void RenderText(string text)
		{
			ioText.Inlines.Clear();
			ioText.Inlines.Add(new Run($"{text}"));
			ioText.InvalidateVisual();
		}

		void RenderData(StdOutErr data)
		{
			ioText.Inlines.Clear();

			bool outTrimTop = true;
			bool errTrimTop = true;
			bool anyErr = false;

			foreach (var x in data.StdErr)
			{
				if (errTrimTop && x.Trim().Replace("\n", string.Empty).Replace("\r", string.Empty).Length is 0)
				{
					outTrimTop = false;
					continue;
				}

				ioText.Inlines.Add(new Run($"{x}") { Foreground = errorBrush });
				anyErr = true;
			}

			if (anyErr)
			{
				ioText.Inlines.Add(new Run($"{Environment.NewLine}"));
			}

			foreach (var x in data.StdOut)
			{
				if (outTrimTop && x.Trim().Replace("\n", string.Empty).Replace("\r", string.Empty).Length is 0)
				{
					outTrimTop = false;
					continue;
				}

				ioText.Inlines.Add(new Run($"{x}"));
			}

			ioText.InvalidateVisual();
		}

		private async void Button_Click(object sender, RoutedEventArgs e)
		{
			if (busy)
			{
				RenderWarning("Minfold in progress, please wait..");
				return;
			}

			busy = true;
			SetIoSize();
			RenderText("Minfold in progress..");

			if (minfoldAvailable is null)
			{
				DataOrException<bool> available = await ProcessDispatcher.Available("minfold");

				if (available.Exception is not null)
				{
					RenderException(available.Exception, "Failed to locate whether minfold is available");
					busy = false;
					return;
				}

				if (!available.Data)
				{
					DataOrException<bool> dotnetAvailable = await ProcessDispatcher.Available("dotnet");

					if (dotnetAvailable.Exception is not null)
					{
						RenderException(dotnetAvailable.Exception, "Dotnet is not available, please install .NET SDK");
						busy = false;
						return;
					}

					DataOrException<StdOutErr> installResult = await ProcessDispatcher.Run("dotnet", "tool install Minfold.Cli --global");

					if (installResult.Exception is not null)
					{
						RenderException(installResult.Exception, "Failed to install Minfold via dotnet tool");
						busy = false;
						return;
					}

					minfoldAvailable = true;
				}
				else
				{
					minfoldAvailable = true;
				}
			}

			if (minfoldAvailable is not true)
			{
				RenderError("Minfold is not available and couldn't be installed automatically. Please install manually via: dotnet tool install Minfold.Cli --global");
				busy = false;
				return;
			}

			List<string> argsBuilder = new List<string>();

			string db = inputDb.Input.RealText.Trim();
			string conn = inputConn.Input.RealText.Trim();
			string path = inputPath.Input.RealText.Trim();
			string args = inputArgs.Input.RealText.Trim();

			if (db.Length > 0)
			{
				argsBuilder.Add($"--database \"{db}\"");
			}

            if (conn.Length > 0)
            {
				argsBuilder.Add($"--connection \"{conn}\"");
			}

			if (path.Length > 0)
			{
				argsBuilder.Add($"--codePath \"{path}\"");
			}

			if (args.Length > 0)
			{
				argsBuilder.Add(args);
			}

			if (argsBuilder.Count is 0)
			{
				argsBuilder.Add("--help");
			}

			DataOrException<StdOutErr> data = await ProcessDispatcher.Run("minfold", string.Join(" ", argsBuilder));

			if (data.Exception is not null)
			{
				RenderException(data.Exception, "Minfold crashed or isn't available");
				busy = false;
				return;
			}

			RenderData(data.Data);
			busy = false;
		}

		private void UserControl_SizeChanged(object sender, SizeChangedEventArgs e)
		{
			SetIoSize();
		}
	}
}
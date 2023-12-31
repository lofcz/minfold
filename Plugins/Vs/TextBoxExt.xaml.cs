﻿using System;
using System.Collections.Generic;
using System.Drawing.Text;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Media.Media3D;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace MinfoldVs
{
	/// <summary>
	/// Interaction logic for TextBoxExt.xaml
	/// </summary>
	public partial class TextBoxExt : TextBox, IDisposable
	{
		public static readonly DependencyProperty ProtectContentDependency = DependencyProperty.Register(nameof(ProtectContent), typeof(bool), typeof(TextBoxExt), new UIPropertyMetadata(null));

		public string RealText { get; set; } = string.Empty;
		public bool ProtectContent
		{
			get { return (bool)GetValue(ProtectContentDependency); }
			set
			{
				SetValue(ProtectContentDependency, value);
			}
		}

		public TextBoxExt()
		{
			InitializeComponent();

			GotFocus += OnFocus;
			LostFocus += OnFocusLost;
		}

		void MaskText()
		{
			if (ProtectContent)
			{
				if (Text.Length > 0)
				{
					Text = "**************************";
				}
			}
		}

		void OnFocusLost(object sender, EventArgs e)
		{
			RealText = Text;
			MaskText();
		}

		void OnFocus(object sender, EventArgs e)
		{
			if (ProtectContent)
			{
				if (RealText.Length > 0)
				{
					Text = RealText;
				}
			}
		}

		public void Dispose()
		{
			GotFocus -= OnFocus;
			LostFocus -= OnFocusLost;
		}

		public void SetText(string text)
		{
			Text = text;
			RealText = text;
			MaskText();
		}
	}
}

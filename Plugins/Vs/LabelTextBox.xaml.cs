using System;
using System.Collections.Generic;
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
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace MinfoldVs
{
	/// <summary>
	/// Interaction logic for LabelTextBox.xaml
	/// </summary>
	public partial class LabelTextBox : UserControl
	{
		public LabelTextBox()
		{
			InitializeComponent();
		}

		public static readonly DependencyProperty LabelDependency = DependencyProperty.Register(nameof(Label), typeof(string), typeof(LabelTextBox), new UIPropertyMetadata(null));
		public static readonly DependencyProperty PlaceholderDependeny = DependencyProperty.Register(nameof(Placeholder), typeof(string), typeof(LabelTextBox), new UIPropertyMetadata(null));


		public string Label
		{
			get { return (string)GetValue(LabelDependency); }
			set
			{
				SetValue(LabelDependency, value);
			}
		}

		public string Placeholder
		{
			get { return (string)GetValue(PlaceholderDependeny); }
			set
			{
				SetValue(PlaceholderDependeny, value);
			}
		}
		
	}
}

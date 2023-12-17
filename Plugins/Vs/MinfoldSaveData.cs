using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MinfoldVs
{
	internal class MinfoldSaveData
	{
		public List<MinfoldSaveDataEntry> saves { get; set; } = [];
	}

	internal class MinfoldSaveDataEntry
	{
		public string connString { get; set; }
		public string database { get; set; }
		public string location { get; set; }
		public string optional { get; set; }
		public string projectPath { get; set; }
		public string projectName { get; set; }

		public MinfoldSaveDataEntry()
		{

		}

		public MinfoldSaveDataEntry(string connString, string database, string location, string optional, string projectPath, string projectName)
		{
			this.connString = connString;
			this.database = database;
			this.location = location;
			this.optional = optional;
			this.projectPath = projectPath;
			this.projectName = projectName;
		}
	}
}

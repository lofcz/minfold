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
		string connString { get; set; }
		string database { get; set; }
		string location { get; set; }
		string optional { get; set; }
		string projectPath { get; set; }
		string projectName { get; set; }

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
